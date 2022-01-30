#include "Vertica.h"
#include <clickhouse/client.h>
#include <curl/curl.h>
#include "curl_fopen.h"
#include "utils.h"

#include <regex>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <unistd.h>

#include "backtrace.h"
INSTALL_SIG_TRAP

#include "access_private.h"
ACCESS_PRIVATE_FIELD(Vertica::VerticaBlock, int, index);


using namespace Vertica;
using namespace std;
using namespace clickhouse;


struct TableColumn
{
    string name;
    string expr;
    int type;
    int size;
    int no;
    bool isUsed;
    bool isMaterialized;
};


class ClickhouseParser : public UDParser
{
    int partNo;
    string colFilter;
    string rowFilter;
    string semijoin;
    string groupby;
    string limit;
    unordered_map<string, string> colExpr;
    unordered_map<string, string> colAggr;
    int rowCountColumn;

    string table;
    vector<TableColumn> columns;
    string query;
    string filterTable;

    Client *client;

public:
    ClickhouseParser(int partNo, string columns, string filter, string semijoin, string groupby, string limit,
                     unordered_map<string, string> colExpr, unordered_map<string, string> colAggr)
        : partNo(partNo), colFilter(columns), rowFilter(filter), semijoin(semijoin), groupby(groupby), limit(limit),
          colExpr(colExpr), colAggr(colAggr), rowCountColumn(-1), client(NULL)
    {}

    void setup(ServerInterface &srvInterface, SizedColumnTypes &returnType)
    {
        ClientOptions opt;
        if (limit.empty())
        {
            opt.SetCompressionMethod(CompressionMethod::LZ4);
        }

        std::vector<string> hostAlt;
        std::vector<string> predicates;
        ParamReader paramReader = srvInterface.getParamReader();
        auto params = getParams(paramReader, DSN_SELECT_DEFAULT_STR);
        if (params.count("host"))
        {
            string hostList = params["host"];
            vector<string> hosts = splitString(hostList, ',');
            if (partNo < 0 || hosts.size() <= (size_t)partNo)
            {
                return;
            }

            string host = hosts[partNo];
            vector<string> hostPts = splitString(host, '[');
            if (hostPts.size() == 2 && host[host.size() - 1] == ']') 
            {
                predicates.push_back(hostPts[1].substr(0, hostPts[1].size() - 1));
                host = hostPts[0];
            }

            hostAlt = splitString(host, '=');
            opt.SetHost(hostAlt[0]);
        }
        if (params.count("port"))
        {
            opt.SetPort(std::strtol(params["port"].c_str(), nullptr, 10));
        }
        if (params.count("database"))
        {
            opt.SetDefaultDatabase(params["database"]);
        }
        if (params.count("user"))
        {
            opt.SetUser(params["user"]);
        }
        if (params.count("password"))
        {
            opt.SetPassword(params["password"]);
        }

        vector<string> colLookup;
        if (colFilter != "*")
        {
            colLookup = splitString(toLower(colFilter), ',');
        }

        this->table = params["relation"];
        this->columns.clear();
        map<string, int> columnTypes;
        for (size_t i = 0; i < returnType.getColumnCount(); ++i)
        {
            string lowName = toLower(returnType.getColumnName(i));

            TableColumn c;
            c.name = returnType.getColumnName(i);
            c.expr = c.name;
            c.type = (int)returnType.getColumnType(i).getTypeOid();
            c.size = max(0, returnType.getColumnType(i).getTypeMod() - 4);
            c.isUsed = (colLookup.empty() || find(colLookup.begin(), colLookup.end(), lowName) != colLookup.end());
            c.no = -1;

            if (colExpr.find(lowName) != colExpr.end())
            {
                c.expr = colExpr[lowName];
                c.isMaterialized = c.isUsed;
            }
            else if (colAggr.find(lowName) != colAggr.end())
            {
                c.expr = colAggr[lowName];
                c.isMaterialized = false;
            }
            else
            {
                c.isMaterialized = c.isUsed;
            }

            this->columns.push_back(c);
            columnTypes.insert(make_pair(lowName, c.type));
        }

        if (!rowFilter.empty())
        {
            predicates.push_back(rowFilter);
        }

        // format query
        stringstream qs;
        qs << "select ";

        int colNo = 0;

        string projectColumns;
        for (size_t i = 0; i < columns.size(); ++i)
        {
            if (!columns[i].isMaterialized) continue;
            columns[i].no = colNo++;
            qs << (columns[i].no ? ", " : "") + columns[i].expr + " as " + columns[i].name;
            projectColumns += (columns[i].no ? ", " : "") + columns[i].expr;
        }

        for (size_t i = 0; i < columns.size(); ++i)
        {
            if (!columns[i].isUsed || columns[i].isMaterialized) continue;
            columns[i].no = colNo++;
            qs << (columns[i].no ? ", " : "") + columns[i].expr;
        }

        qs << " from " << table;
        for (size_t i = 0; i < predicates.size(); ++ i)
        {
            qs << (i > 0 ? " and " : " where ");
            qs << predicates[i];
        }

        filterTable.clear();
        if (!semijoin.empty())
        {
            regex regexp("(.*)\\s+in\\s*\\(.*Clickhouse.*?relation\\s*=\\s*'([^']*)'",
                         regex_constants::icase);
            smatch m;
            if (regex_search(semijoin, m, regexp))
            {
                std::string semijoinCols = m[1].str();
                filterTable = m[2].str();

                std::string sid = getSessionId(srvInterface);
                filterTable += "_" + splitPair(sid, ':').back();
                srvInterface.log("filter table: %s", filterTable.c_str());

                qs << (predicates.size() > 0 ? " and " : " where ");
                qs << semijoinCols + " in " + filterTable;
            }
        }

        if (!groupby.empty() && !projectColumns.empty())
        {
            qs << " group by " << projectColumns;
        }
        if (!limit.empty())
        {
            qs << " limit " << limit;
        }

        this->query = qs.str();

        qs << " [" << opt << "]";
        srvInterface.log("%s", qs.str().c_str());

        // connect to clickhouse
        client = createClickhouseClient(opt, hostAlt, srvInterface);
    }

    StreamState process(ServerInterface &srvInterface, DataBuffer &input, InputState input_state)
    {
        if (!client)
        {
            return DONE;
        }

        string staticRowFilter = rowFilter;
        set<string> predefColExpr;
        unordered_map<string, string> colExprType;

        if (!rowFilter.empty())
        {
            srvInterface.log("check calculated columns...");

            client->Select(
                "select name, default_expression, type "
                "from system.columns "
                "where database || '.' || table = '" + table + "' and default_kind = 'ALIAS'",
                [&](const Block& block)
                {
                    for (size_t i = 0; i < block.GetRowCount(); ++i)
                    {
                        string name(block[0]->As<ColumnString>()->At(i));
                        string expr(block[1]->As<ColumnString>()->At(i));
                        string type(block[2]->As<ColumnString>()->At(i));

                        colExpr[name] = expr;
                        colExprType[name] = type;
                        predefColExpr.insert(name);
                    }
                }
            );

            for (auto pair : colExpr)
            {
                auto exprType = colExprType.find(pair.first);
                if (exprType != colExprType.end() && exprType->second == "UInt8")
                {
                    string pattern("\\b" + pair.first + "\\b(\\s*(=|<|>|<=|>=|<>|!=)\\s*[0-9]+)?");
                    staticRowFilter = replaceSubstring(staticRowFilter, pattern, "1 = 1");
                    continue; // allow boolean filters even for calculated fields
                }
                if (pair.second.find("dictGet") == string::npos)
                {
                    string pattern("\\b" + pair.first + "\\b");
                    staticRowFilter = replaceSubstring(staticRowFilter, pattern, pair.second);
                    rowFilter = replaceSubstring(rowFilter, pattern, pair.second);
                    continue; // allow calculated fields without dictGet
                }
                if (regex_search(rowFilter, regex("\\b" + pair.first + "\\b")))
                {
                    vt_report_error(0, "calculated columns are not allowed in filter");
                }
            }
        }

        bool tooManyPartitions = false;
        string partitionColumns;
        string partitionValues;
        string partitionClause = "1 = 1";

        if (srvInterface.getParamReader().containsParameter("warmup_partitions"))
        {
            srvInterface.log("check partition count...");

            int minPartitions = 7;
            int maxPartitions = srvInterface.getParamReader().getIntRef("warmup_partitions");

            int colCnt = 0;
            for (size_t i = 0; i < columns.size(); ++i)
            {
                colCnt += (columns[i].isUsed ? 1 : 0);
            }
            maxPartitions = ceil(minPartitions + max(0, maxPartitions - minPartitions)
                                 / exp_compat(0.5 * (colCnt * colCnt) / (100 * 100)));

            client->Select(
                "select arrayStringConcat(groupArray(name), ',') "
                "from system.columns "
                "where database || '.' || table = '" + table + "' and is_in_partition_key",
                [&](const Block& block)
                {
                    for (size_t i = 0; i < block.GetRowCount(); ++i)
                    {
                        partitionColumns = block[0]->As<ColumnString>()->At(i);
                        srvInterface.log("partition columns: %s", partitionColumns.c_str());
                    }
                }
            );

            client->Select(
                "select count(*), arrayStringConcat(groupArray(pt), ',') "
                "from ( "
                "    select toString(tuple(" + partitionColumns + ")) as pt "
                "    from " + table +
                "    where " + (staticRowFilter.empty() ? "1=1" : staticRowFilter) +
                "    group by pt "
                ") t ",
                [&](const Block& block)
                {
                    for (size_t i = 0; i < block.GetRowCount(); ++i)
                    {
                        int cnt = block[0]->As<ColumnUInt64>()->At(i);
                        partitionValues = block[1]->As<ColumnString>()->At(i);
                        srvInterface.log("partition count: %d [max=%d]", cnt, maxPartitions);
                        tooManyPartitions = (cnt > maxPartitions);
                    }
                }
            );

            if (partitionValues.size() > 0)
            {
                partitionClause = "(" + partitionColumns + ") in (" + partitionValues + ")";
            }
            else
            {
                partitionClause = "1 = 0";
            }
            query = regex_replace(query, regex(" where "), " where " + partitionClause + " and ");
        }

        if (srvInterface.getParamReader().containsParameter("warmup_cache")
            && srvInterface.getParamReader().getBoolRef("warmup_cache"))
        {
            srvInterface.log("cache warming...");

            set<string> cacheDicts;

            client->Select(
                "select name from system.dictionaries where type like '%Cache%' and source not like '%.so%'",
                [&](const Block& block)
                {
                    for (size_t i = 0; i < block.GetRowCount(); ++i)
                    {
                        string n(block[0]->As<ColumnString>()->At(i));
                        cacheDicts.insert(n);
                    }
                }
            );

            set<string> columns, usedColumns, addedColumns;
            unordered_map<string, string> usedColExpr;
            for (auto c : this->columns)
            {
                columns.insert(c.name);
                auto exprIter = colExpr.find(toLower(c.name));
                if (c.isUsed)
                {
                    usedColumns.insert(c.name);
                }
                if (exprIter != colExpr.end() && predefColExpr.find(toLower(c.name)) == predefColExpr.end())
                {
                    addedColumns.insert(c.name);
                }
                if (c.isUsed && exprIter != colExpr.end())
                {
                    usedColExpr[c.name] = exprIter->second;
                }
            }

            for (auto pair : this->colExpr)
            {
                string column = pair.first;
                string expr = pair.second;
                smatch m;

                regex regexp("dictGet\\(\\s*[^,]+,\\s*[^,]+,\\s*([a-zA-Z0-9]+\\(([^()]+|[^()]*\\([^()]+\\))\\)|\\([^()]+\\)|[^()]*)\\s*\\)");
                if (expr.find("dictGet") == string::npos || !regex_search(expr, m, regexp) ||
                    !any_of(begin(cacheDicts), end(cacheDicts), [&](const string& n){return regex_search(expr, regex("\\b" + n + "\\b"));}))
                {
                    continue; // no dictionary column
                }

                string dictExpr = m[0].str();
                string dictArg = m[1].str();

                if (usedColumns.find(column) == usedColumns.end() && all_of(begin(usedColExpr), end(usedColExpr),
                    [&](auto &ce) {return !regex_search(ce.second, regex("\\b" + column + "\\b")) || column == ce.first;}))
                {
                    continue; // not used directly and not mentioned in another column
                }

                if (any_of(begin(addedColumns), end(addedColumns),
                    [&](const string& c){return regex_search(dictArg, regex("\\b" + c + "\\b")) && column != c;}))
                {
                    continue; // has reference to another calculated column
                }

                if (tooManyPartitions)
                {
                    vt_report_error(0, "Too many partitions with cache dictionary. \n"
                                       "Avoid dictGet columns [%s] or set filter on partition [%s]",
                                       column.c_str(), partitionColumns.c_str());
                }

                if (limit.empty() && !rowFilter.empty())
                {
                    string warmupQuery =
                            "select " + dictExpr
                            + " from " + table
                            + " where " + staticRowFilter + " and " + partitionClause
                            + " group by " + dictArg;
                    srvInterface.log("warmup '%s': %s", column.c_str(), warmupQuery.c_str());
                    client->Execute("select count(*) from (" + warmupQuery + ") t");
                }
            }
            srvInterface.log("cache reloaded");
        }

        if (!filterTable.empty())
        {
            srvInterface.log("waiting for semijoin");

            int waitTimeout = 300;
            int waitSleep = 10;
            while (waitTimeout > 0)
            {
                bool waitForInsert = false;
                client->Select(
                    "select 1 "
                    "from system.tables "
                    "where database || '.' || name = '" + filterTable + "' "
                    "  and total_rows = 0 "
                    "union all "
                    "select 1 "
                    "from system.replicas "
                    "where (inserts_in_queue <> 0 or absolute_delay <> 0 or future_parts <> 0) "
                    "  and database || '.' || table = '" + filterTable + "' "
                    "union all "
                    "select 1 "
                    "from system.replication_queue "
                    "where database || '.' || table = '" + filterTable + "' ",
                    [&](const Block& block)
                {
                    waitForInsert = block.GetRowCount() > 0;
                });
                if (!waitForInsert)
                {
                    break;
                }
                srvInterface.log("not ready yet...");
                waitTimeout -= waitSleep;
                sleep(waitSleep);
            }
            if (waitTimeout <= 0)
            {
                vt_report_error(0, "Timed out waiting for semijoin data [%s]",
                                filterTable.c_str());
            }
            srvInterface.log("semijoin data awailable");
        }

        client->Execute(Query(this->query).OnDataCancelable([this,&srvInterface](const Block& block) -> bool
        {
            // srvInterface->log("block fetched: %zu", block.GetRowCount());

            int tzLocalShift = getTZLocalShift();
            const vint nullValue = srvInterface.getParamReader().containsParameter("zero2null") &&
                                   srvInterface.getParamReader().getBoolRef("zero2null") ? 0 : vint_null;
            vector<Column*> blockColumns(block.GetColumnCount(), 0);
            int batchSize = writer->getNumRows(); // target block size

            for (size_t chRow = 0; chRow < block.GetRowCount(); chRow += batchSize)
            {
                if (isCanceled()) return false;

                batchSize = min((int)(block.GetRowCount() - chRow), writer->getNumRows()); // target block size

                for (size_t col = 0; col < columns.size(); ++col)
                {
                    size_t chCol = (size_t) columns[col].no;

                    // fill pruned columns
                    if (!columns[col].isUsed || chCol >= block.GetColumnCount())
                    {
                        if (columns[col].type == VarcharOID)
                        {
                            char *column = writer->getColPtrForWrite<char>(col);
                            int elemSize = writer->getColStride(col);

                            for (int i = 0; i < batchSize; ++i)
                            {
                                *(int64*)(column + i * elemSize) = 0x00000000ffffffff;
                            }
                        }
                        continue;
                    }

                    // get column
                    if (blockColumns[chCol] == 0)
                    {
                        blockColumns[chCol] = block[chCol].get();
                    }
                    Column *colRef = blockColumns[chCol];
                    int chType = colRef->GetType().GetCode();

                    // unwrap nulls
                    ColumnUInt8 *nullRef = NULL; //TODO: make use of nulls
                    if (chType == Type::Nullable)
                    {
                        auto colData = reinterpret_cast<ColumnNullable*>(colRef);
                        nullRef = reinterpret_cast<ColumnUInt8*>(colData->Nulls().get());
                        colRef = colData->Nested().get();
                        chType = colRef->GetType().GetCode();
                    }

                    // srvInterface.log("col %s [%d <- %d]: %d <- %d", 
                    //     columns[col].name.c_str(), col, chCol, columns[col].type, chType);

                    // copy primitive type
                    if (columns[col].type == Int8OID && chType == Type::Int8)
                    {
                        ColumnInt8 *src = reinterpret_cast<ColumnInt8*>(colRef);
                        vint *dst = writer->getColPtrForWrite<vint>(col);
                        for (int i = 0; i < batchSize; ++i)
                        {
                            //todo: dont null bool flags
                            dst[i] = (src->At(chRow + i) == nullValue ? vint_null : src->At(chRow + i));
                        }
                    }
                    else if (columns[col].type == Int8OID && chType == Type::Int16)
                    {
                        ColumnInt16 *src = reinterpret_cast<ColumnInt16*>(colRef);
                        vint *dst = writer->getColPtrForWrite<vint>(col);
                        for (int i = 0; i < batchSize; ++i)
                        {
                            dst[i] = (src->At(chRow + i) == nullValue ? vint_null : src->At(chRow + i));
                        }
                    }
                    else if (columns[col].type == Int8OID && chType == Type::Int32)
                    {
                        ColumnInt32 *src = reinterpret_cast<ColumnInt32*>(colRef);
                        vint *dst = writer->getColPtrForWrite<vint>(col);
                        for (int i = 0; i < batchSize; ++i)
                        {
                            dst[i] = (src->At(chRow + i) == nullValue ? vint_null : src->At(chRow + i));
                        }
                    }
                    else if (columns[col].type == Int8OID && chType == Type::Int64)
                    {
                        ColumnInt64 *src = reinterpret_cast<ColumnInt64*>(colRef);
                        vint *dst = writer->getColPtrForWrite<vint>(col);
                        for (int i = 0; i < batchSize; ++i)
                        {
                            dst[i] = (src->At(chRow + i) == nullValue ? vint_null : src->At(chRow + i));
                        }
                    }
                    else if (columns[col].type == Int8OID && chType == Type::UInt8)
                    {
                        ColumnUInt8 *src = reinterpret_cast<ColumnUInt8*>(colRef);
                        vint *dst = writer->getColPtrForWrite<vint>(col);
                        for (int i = 0; i < batchSize; ++i)
                        {
                            //todo: dont null bool flags
                            dst[i] = (src->At(chRow + i) == nullValue ? vint_null : src->At(chRow + i));
                        }
                    }
                    else if (columns[col].type == Int8OID && chType == Type::UInt16)
                    {
                        ColumnUInt16 *src = reinterpret_cast<ColumnUInt16*>(colRef);
                        vint *dst = writer->getColPtrForWrite<vint>(col);
                        for (int i = 0; i < batchSize; ++i)
                        {
                            dst[i] = (src->At(chRow + i) == nullValue ? vint_null : src->At(chRow + i));
                        }
                    }
                    else if (columns[col].type == Int8OID && chType == Type::UInt32)
                    {
                        ColumnUInt32 *src = reinterpret_cast<ColumnUInt32*>(colRef);
                        vint *dst = writer->getColPtrForWrite<vint>(col);
                        for (int i = 0; i < batchSize; ++i)
                        {
                            dst[i] = (src->At(chRow + i) == nullValue ? vint_null : src->At(chRow + i));
                        }
                    }
                    else if (columns[col].type == Int8OID && chType == Type::UInt64)
                    {
                        ColumnUInt64 *src = reinterpret_cast<ColumnUInt64*>(colRef);
                        vint *dst = writer->getColPtrForWrite<vint>(col);
                        for (int i = 0; i < batchSize; ++i)
                        {
                            dst[i] = (src->At(chRow + i) == nullValue ? vint_null : src->At(chRow + i));
                        }
                    }
                    else if (columns[col].type == TimestampOID && chType == Type::DateTime)
                    {
                        ColumnDateTime *src = reinterpret_cast<ColumnDateTime*>(colRef);
                        Timestamp *dst = writer->getColPtrForWrite<Timestamp>(col);
                        for (int i = 0; i < batchSize; ++i)
                        {
                            dst[i] = (src->At(chRow + i) == nullValue ? vint_null :
                                      getTimestampFromUnixTime(src->At(chRow + i) + tzLocalShift));
                        }
                    }
                    else if (columns[col].type == DateOID && chType == Type::Date)
                    {
                        ColumnDate *src = reinterpret_cast<ColumnDate*>(colRef);
                        DateADT *dst = writer->getColPtrForWrite<DateADT>(col);
                        for (int i = 0; i < batchSize; ++i)
                        {
                            dst[i] = (src->At(chRow + i) == nullValue ? vint_null :
                                      getDateFromUnixTime(src->At(chRow + i)));
                        }
                    }
                    else if (columns[col].type == Float8OID && chType == Type::Float32)
                    {
                        ColumnFloat32 *src = reinterpret_cast<ColumnFloat32*>(colRef);
                        vfloat *dst = writer->getColPtrForWrite<vfloat>(col);
                        for (int i = 0; i < batchSize; ++i)
                        {
                            dst[i] = (src->At(chRow + i) == nullValue ? vfloat_null :
                                      src->At(chRow + i));
                        }
                    }
                    else if (columns[col].type == Float8OID && chType == Type::Float64)
                    {
                        ColumnFloat64 *src = reinterpret_cast<ColumnFloat64*>(colRef);
                        vfloat *dst = writer->getColPtrForWrite<vfloat>(col);
                        for (int i = 0; i < batchSize; ++i)
                        {
                            dst[i] = (src->At(chRow + i) == nullValue ? vfloat_null :
                                      src->At(chRow + i));
                        }
                    }
                    else if (columns[col].type == NumericOID && (chType == Type::Decimal64 || chType == Type::Decimal32 || chType == Type::Decimal))
                    {
                        ColumnDecimal *src = reinterpret_cast<ColumnDecimal*>(colRef);
                        char *dstBytes = writer->getColPtrForWrite<char>(col);
                        int dstStride = writer->getColStride(col);

                        for (int i = 0; i < batchSize; ++i)
                        {
                            Int128 v = src->At(chRow + i);
                            uint64 *dst = reinterpret_cast<uint64 *>(dstBytes + i * dstStride);
                            *dst = v;
                        }
                    }
                    else if (columns[col].type == VarcharOID && chType == Type::String)
                    {
                        ColumnString *src = reinterpret_cast<ColumnString*>(colRef);
                        char *dstBytes = writer->getColPtrForWrite<char>(col);
                        int dstStride = writer->getColStride(col);

                        for (int i = 0; i < batchSize; ++i)
                        {
                            string_view v(src->At(chRow + i));
                            EE::StringValue *dst = reinterpret_cast<EE::StringValue*>(dstBytes + i * dstStride);
                            dst->slen = min((vsize)v.size(), (vsize)columns[col].size);                 
                            if (nullValue == 0 && dst->slen == 0)
                            {
                                dst->slen = StringNull;
                                dst->sloc = 0;
                            }
                            else
                            {
                                strncpy(&dst->base[dst->sloc], v.data(), dst->slen);
                            }
                        }
                    }
                    else if (columns[col].type == VarcharOID && chType == Type::FixedString)
                    {
                        ColumnFixedString *src = reinterpret_cast<ColumnFixedString*>(colRef);
                        char *dstBytes = writer->getColPtrForWrite<char>(col);
                        int dstStride = writer->getColStride(col);

                        for (int i = 0; i < batchSize; ++i)
                        {
                            string_view v(src->At(chRow + i));
                            EE::StringValue *dst = reinterpret_cast<EE::StringValue*>(dstBytes + i * dstStride);
                            dst->slen = min((vsize)v.size(), (vsize)columns[col].size);
                            if (nullValue == 0 && dst->slen == 0)
                            {
                                dst->slen = StringNull;
                                dst->sloc = 0;
                            }
                            else
                            {
                                strncpy(&dst->base[dst->sloc], v.data(), dst->slen);
                            }
                        }
                    }
                    else
                    {
                        writer->setNull(col);

                        char *column = writer->getColPtrForWrite<char>(col);
                        int elemSize = writer->getColStride(col);
                        for (int i = 1; i < batchSize; ++i)
                        {
                            memcpy(column + i * elemSize, column, elemSize);
                        }
                    }
                }

                access_private::index(*writer) += batchSize;
                writer->getWriteableBlock();

                // for (int i = 0; i < batchSize; ++i)
                // {
                //     writer->next();
                // }
            }

            return true;
        }
        ));
        return DONE;
    }

    void destroy(ServerInterface &srvInterface, SizedColumnTypes &returnType)
    {
        if (!filterTable.empty() && this->client)
        {
            std::string sid = getSessionId(srvInterface);
            std::string suffix = splitPair(sid, ':').back();

            if (suffix.size() > 2 && filterTable.find(suffix) != std::string::npos)
            {
                srvInterface.log("drop %s", filterTable.c_str());
                //this->client->Execute("drop table if exists " + filterTable);
            }

            filterTable.clear();
        }

        delete this->client;
        this->client = NULL;
    }
};

class ClickhouseParserFactory : public ParserFactory {
    static unordered_map<vint, int> txnThreads;
    static std::mutex mutex;

public:
    void plan(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt)
    {
        vint t = chrono::duration_cast<chrono::milliseconds>(
                    chrono::system_clock::now().time_since_epoch()).count();
        vint txnNo = t * 1000LL + getpid() % 1000LL;
        planCtxt.getWriter().setInt("txn", txnNo);

        string sessionId = getSessionId(srvInterface.getCurrentNodeName());
        srvInterface.log("sessionId: %s", sessionId.c_str());

        ParamReader paramReader = srvInterface.getParamReader();
        if (!paramReader.containsParameter("relation"))
        {
            vt_report_error(0, "relation name is required");
        }
        auto params = getParams(paramReader, DSN_SELECT_DEFAULT_STR);

        if (paramReader.containsParameter("filter"))
        {
            string filter = paramReader.getStringRef("filter").str();
            srvInterface.log("filter: %s", filter.c_str());
            planCtxt.getWriter().getStringRef("filter").copy(filter);
        }

        if (!params["storage_access_resolver"].empty())
        {
            string path = params["storage_access_resolver"];
            string table = params["relation"];

            string url = path + "/" + sessionId + "?table=" + table;
            if (params.count("id") != 0)
            {
                url += "&id=" + params["id"];
            }
            srvInterface.log("%s", url.c_str());

            const int buf_size = 32000;
            char buf[buf_size];

            URL_FILE *handle = url_fopen(url.c_str(),"r");
            if (!handle)
            {
                vt_report_error(0, "failed to access: %s", url.c_str());
            }
            int size = url_fread(buf, 1, buf_size, handle);

            string storageAccess(buf, size);

            srvInterface.log("storage access:");
            for (string pair: splitString(storageAccess, '\n'))
            {
                vector<string> parts = splitPair(pair, ':');
                if (parts.size() == 2 && !parts[1].empty())
                {
                    srvInterface.log("%s: %s", parts[0].c_str(), parts[1].c_str());
                    planCtxt.getWriter().getStringRef(parts[0]).copy(parts[1]);
                }
            }

            if (!planCtxt.getWriter().containsParameter("columns") &&
                !planCtxt.getWriter().containsParameter("expr") &&
                !planCtxt.getWriter().containsParameter("groupby"))
            {
                vt_report_error(0, "storage access failed:\n %s", storageAccess.c_str());
            }

            if (!planCtxt.getWriter().containsParameter("columns"))
            {
                srvInterface.log("columns: NONE");
                planCtxt.getWriter().getStringRef("columns").copy("NONE");
            }

            if (planCtxt.getWriter().containsParameter("expr")
                && planCtxt.getWriter().containsParameter("groupby"))
            {
                string expressions = planCtxt.getReader().getStringRef("expr").str();

                for (string ex: splitString(toLower(expressions), ';'))
                {
                    for (string agg: {"count"})
                    {
                        if (ex.find(agg + "(") != string::npos && ex.find("row_count") == string::npos)
                        {
                            vt_report_error(0, "plan error:\n %s() cannot be pushed.\n"
                                            "consider using transitive functions like sum(row_count).\n"
                                            "you need to add row_count column to do this", agg.c_str());
                        }
                    }

                    for (string agg: {"avg", "sum"})
                    {
                        if (ex.find(agg + "(") != string::npos && ex.find("row_count") == string::npos)
                        {
                            vt_report_error(0, "plan error:\n use %s(row_count * <expr>) to get correct result.\n "
                                            "you need to add row_count column to do this", agg.c_str());
                        }
                    }
                }
            }
        }
    }

    UDParser* prepare(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt,
            const SizedColumnTypes &returnType)
    {
        mutex.lock();
        vint txn = planCtxt.getReader().getIntRef("txn");
        auto txnIt = txnThreads.find(txn);
        int threadNo = (txnIt != txnThreads.end() ? txnIt->second : 0);
        txnThreads[txn] = threadNo + 1;
        mutex.unlock();

        string node = srvInterface.getCurrentNodeName();
        vector<string> nodes = planCtxt.getClusterNodes();

        sort(nodes.begin(), nodes.end());

        int partNo = 0;
        auto it = std::find(nodes.begin(), nodes.end(), node);
        if (it != nodes.end())
        {
            partNo = distance(nodes.begin(), it);
        }

        partNo += threadNo * nodes.size();
        srvInterface.log("txn=%lld threadNo=%d partNo=%d", txn, threadNo, partNo);

        string columns, formalColumns, filter, semijoin, expr, groupby, limit;
        if (planCtxt.getReader().containsParameter("columns"))
        {
            columns = planCtxt.getReader().getStringRef("columns").str();
        }
        if (planCtxt.getReader().containsParameter("formal_columns"))
        {
            formalColumns = planCtxt.getReader().getStringRef("formal_columns").str();
        }
        if (planCtxt.getReader().containsParameter("filter"))
        {
            filter = planCtxt.getReader().getStringRef("filter").str();
        }
        if (planCtxt.getReader().containsParameter("semijoin"))
        {
            semijoin = planCtxt.getReader().getStringRef("semijoin").str();
        }
        if (planCtxt.getReader().containsParameter("expr"))
        {
            expr = planCtxt.getReader().getStringRef("expr").str();
        }
        if (planCtxt.getReader().containsParameter("groupby"))
        {
            groupby = planCtxt.getReader().getStringRef("groupby").str();
        }
        if (planCtxt.getReader().containsParameter("limit"))
        {
            limit = planCtxt.getReader().getStringRef("limit").str();
        }

        unordered_map<string, string> colExpr, colAggr;
        for (string c : perColumnParamReader.getColumnNames())
        {
            ParamReader params = perColumnParamReader.getColumnParamReader(c);
            if (params.containsParameter("format"))
            {
                string expr = params.getStringRef("format").str();
                auto funcs = {"count", "sum", "uniq", "uniqExact", "avg", "min", "max", "any", "argMax", "argMin", "Merge"};

                if (any_of(begin(funcs), end(funcs), [&](string agg){return expr.find(agg + "(") != string::npos;}))
                {
                    colAggr[toLower(c)] = expr;
                }
                else
                {
                    colExpr[toLower(c)] = expr;
                }
            }
        }

        if (!colAggr.empty() && groupby.empty())
        {
            for (string c : perColumnParamReader.getColumnNames())
            {
                if (colAggr.find(toLower(c)) == colAggr.end())
                {
                    groupby += (groupby.empty() ? "" : ", ") + c;
                }
            }
            if (groupby.empty())
            {
                groupby = "NOTHING";
            }
        }

        if (!groupby.empty())
        {
            colAggr["row_count"] = "count(*)";
            vector<string> anyColumns = splitString(formalColumns, ',');
            for (size_t i = 0; i < anyColumns.size(); ++i)
            {
                string c = toLower(anyColumns[i]);
                if (c.size() > 0 && colExpr.find(c) == colExpr.end() && colAggr.find(c) == colAggr.end())
                {
                    colAggr[c] = string("any(") + anyColumns[i] + string(")");
                }
            }
        } 
        else 
        {
            colExpr["row_count"] = "1";
        }

        if (!filter.empty())
        {
            filter = regex_replace(filter, regex("isUTF8\\s*\\(", regex_constants::icase), "isValidUTF8(");
            filter = regex_replace(filter, regex("regexp_like\\s*\\(", regex_constants::icase), "match(");
        }

        return vt_createFuncObject<ClickhouseParser>(srvInterface.allocator, partNo,
            columns, filter, semijoin, groupby, limit, colExpr, colAggr);
    }

    void getParameterType(ServerInterface &srvInterface,
                          SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addVarchar(256, "relation");
        parameterTypes.addVarchar(65000, "filter");
        parameterTypes.addBool("zero2null");
        parameterTypes.addBool("warmup_cache");
        parameterTypes.addInt("warmup_partitions");

        parameterTypes.addVarchar(4096, "host");
        parameterTypes.addInt("port");
        parameterTypes.addVarchar(256, "database");
        parameterTypes.addVarchar(256, "user");
        parameterTypes.addVarchar(256, "password");

        parameterTypes.addVarchar(256, "dsn");
        parameterTypes.addVarchar(256, "dsn_path");

        parameterTypes.addVarchar(256, "storage_access_resolver");
    }

};

mutex ClickhouseParserFactory::mutex;
unordered_map<vint, int> ClickhouseParserFactory::txnThreads;

RegisterFactory(ClickhouseParserFactory);
