#include "Vertica.h"
#include <clickhouse/client.h>
#include <curl/curl.h>
#include "curl_fopen.h"
#include "utils.h"

#include "access_private.h"
ACCESS_PRIVATE_FIELD(clickhouse::ColumnDate, std::shared_ptr<clickhouse::ColumnUInt16>, data_);
ACCESS_PRIVATE_FIELD(clickhouse::ColumnDateTime, std::shared_ptr<clickhouse::ColumnUInt32>, data_);

using namespace Vertica;
using namespace std;
using namespace clickhouse;


struct ColumnData
{
    std::vector<uint8_t>  value_uint8;
    std::vector<uint16_t> value_uint16;
    std::vector<uint32_t> value_uint32;
    std::vector<uint64_t> value_uint64;
    std::vector<int8_t>  value_int8;
    std::vector<int16_t> value_int16;
    std::vector<int32_t> value_int32;
    std::vector<int64_t> value_int64;
    std::vector<float>  value_float;
    std::vector<double> value_double;
    std::vector<string> value_string;
    int last_value_ind;
};


#define allocateColumn(Type, Name, Default) \
    if (!columnValues[i]) \
    { \
        columnValues[i] = new ColumnData(); \
        columnValues[i]->value_ ## Name = vector<Type>(blockSize, Default); \
    }


class ClickhouseExport : public TransformFunction
{
    Client *client;
    std::string tableName;
    unordered_map<std::string, int> targetColumns;
    bool pipelined;
    std::string semijoinTable;

public:
    ClickhouseExport(bool pipelined=false)
        : pipelined(pipelined)
    {
    }

    virtual void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes)
    {
        std::string node = srvInterface.getCurrentNodeName();
        int seed = atoi(node.substr(node.size() - 4).c_str());

        ClientOptions opt;
        opt.SetCompressionMethod(CompressionMethod::LZ4);

        std::vector<string> hostAlt;
        std::vector<string> buddyHostAlt;

        ParamReader paramReader = srvInterface.getParamReader();

        auto params = getParams(paramReader, DSN_INSERT_DEFAULT_STR);

        if (params.count("host"))
        {           
            vector<string> shards = splitString(params["host"], ',');
            string shard = shards[seed % shards.size()];
            hostAlt = splitString(shard, '=');
            opt.SetHost(hostAlt[0]);

            string buddyShard = shards[(seed + 1) % shards.size()];
            buddyHostAlt = splitString(buddyShard, '=');
        }
        else
        {
            vt_report_error(0, "dsn is empty and no host specified");
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
        stringstream ss;
        ss << "connecting to [" << opt << "]";
        srvInterface.log("%s", ss.str().c_str());

        // connect to clickhouse
        client = createClickhouseClient(opt, hostAlt, srvInterface);
        srvInterface.log("client connected: %s", client ? "true" : "false");

        // get columns     
        unordered_map<string, int> typeId;
        typeId["UInt8"] = Type::UInt8;
        typeId["UInt16"] = Type::UInt16;
        typeId["UInt32"] = Type::UInt32;
        typeId["UInt64"] = Type::UInt64;
        typeId["Int8"] = Type::Int8;
        typeId["Int16"] = Type::Int16;
        typeId["Int32"] = Type::Int32;
        typeId["Int64"] = Type::Int64;
        typeId["Float32"] = Type::Float32;
        typeId["Float64"] = Type::Float64;
        typeId["String"] = Type::String;
        typeId["Date"] = Type::Date;
        typeId["DateTime"] = Type::DateTime;

        tableName = srvInterface.getParamReader().getStringRef("relation").str();

        if (pipelined && (!paramReader.containsParameter("ddl") || paramReader.getBoolRef("ddl"))
            || !pipelined && paramReader.containsParameter("ddl") && paramReader.getBoolRef("ddl"))
        {
            srvInterface.log("create table %s", tableName.c_str());

            std::string names;
            std::string colddl;
            for (size_t i = 0; i < argTypes.getColumnCount(); ++i)
            {
                std::string name = argTypes.getColumnName(i);
                const VerticaType &type = argTypes.getColumnType(i);
                std::string typeName(type.isInt() ? "UInt64" : type.getTypeStr());
                colddl += (i == 0 ? "" : ", ") + name + " " + typeName;
                names += (i == 0 ? "" : ", ") + name;
                srvInterface.log("%s %s", name.c_str(), typeName.c_str());
            }

            if (pipelined)
            {
                std::string sid = getSessionId(srvInterface);
                tableName += "_" + splitPair(sid, ':').back();
                srvInterface.log("table name: %s", tableName.c_str());
            }

            vector<Client*> clients;
            clients.push_back(client);

            Client *buddyClient = NULL;
            if (buddyHostAlt[0] != hostAlt[0])
            {
                buddyClient = createClickhouseClient(opt, buddyHostAlt, srvInterface);
                clients.push_back(buddyClient);
            }

            for (Client *cl : clients)
            {
                srvInterface.log("deploy ddl");

                bool hasClusterSetup = true;
                try
                {
                    cl->Select("select getMacro('replica')", [&](const Block& block) {});
                }
                catch(...)
                {
                    hasClusterSetup = false;
                }

                cl->Execute(
                    "create table if not exists " + tableName +
                    "( "
                    + colddl +
                    ") "
                    "engine = " +
                    (hasClusterSetup ?
                           "ReplicatedReplacingMergeTree('/clickhouse/tables/" + tableName + "', '{replica}') " :
                           "ReplacingMergeTree()") +
                    "order by (" + names + ")"
                );
            }

            delete buddyClient;
        }

        srvInterface.log("get columns for %s", tableName.c_str());

        client->Select(
            "select name, type "
            "from system.columns "
            "where database || '.' || table = '" + tableName + "'",
            [&](const Block& block)
            {
                for (size_t i = 0; i < block.GetRowCount(); ++i)
                {
                    string name(block[0]->As<ColumnString>()->At(i));
                    string type(block[1]->As<ColumnString>()->At(i));
                    targetColumns[name] = typeId[type];

                    srvInterface.log("%s: %s", name.c_str(), type.c_str());
                }
            }
        );
    }

    virtual void destroy(ServerInterface &srvInterface, const SizedColumnTypes &argTypes)
    {
        delete this->client;
        this->client = NULL;
    }

    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &inputReader,
                                  PartitionWriter &outputWriter)
    {
        try {

            SizedColumnTypes columns = inputReader.getTypeMetaData();
            std::vector<ColumnData*> columnValues(columns.getColumnCount());
            int blockSize = 1000000;
            int row = 0;
            vint total = 0;

            std::vector<int> types;
            std::vector<string> names;

            for (size_t i = 0; i < columns.getColumnCount(); ++i)
            {
                std::string name = columns.getColumnName(i);
                if (targetColumns.find(name) == targetColumns.end())
                {
                    vt_report_error(0, "missing column %s in target table", name.c_str());
                }
                types.push_back(targetColumns[name]);
                names.push_back(name);
            }

            srvInterface.log("export started for %s", tableName.c_str());

            do
            {
                for (size_t i = 0; i < inputReader.getNumCols(); ++i)
                {
                    if (pipelined)
                    {
                        outputWriter.copyFromInput(i, inputReader, i);
                    }

                    if (inputReader.isNull(i))
                    {
                        continue;
                    }

                    if (types[i] == Type::UInt8)
                    {
                        allocateColumn(uint8_t, uint8, 0)
                        columnValues[i]->value_uint8[row] = inputReader.getIntRef(i);
                    }
                    else if (types[i] == Type::UInt16)
                    {
                        allocateColumn(uint16_t, uint16, 0)
                        columnValues[i]->value_uint16[row] = inputReader.getIntRef(i);
                    }
                    else if (types[i] == Type::UInt32)
                    {
                        allocateColumn(uint32_t, uint32, 0)
                        columnValues[i]->value_uint32[row] = inputReader.getIntRef(i);
                    }
                    else if (types[i] == Type::UInt64)
                    {
                        allocateColumn(uint64_t, uint64, 0);
                        columnValues[i]->value_uint64[row] = inputReader.getIntRef(i);
                    }
                    else if (types[i] == Type::Int8)
                    {
                        allocateColumn(int8_t, int8, 0)
                        columnValues[i]->value_int8[row] = inputReader.getIntRef(i);
                    }
                    else if (types[i] == Type::Int16)
                    {
                        allocateColumn(int16_t, int16, 0)
                        columnValues[i]->value_int16[row] = inputReader.getIntRef(i);
                    }
                    else if (types[i] == Type::Int32)
                    {
                        allocateColumn(int32_t, int32, 0)
                        columnValues[i]->value_int32[row] = inputReader.getIntRef(i);
                    }
                    else if (types[i] == Type::Int64)
                    {
                        allocateColumn(int64_t, int64, 0);
                        columnValues[i]->value_int64[row] = inputReader.getIntRef(i);
                    }
                    else if (types[i] == Type::Float32)
                    {
                        allocateColumn(float, float, 0);
                        columnValues[i]->value_float[row] = columns.getColumnType(i).isNumeric() ?
                            inputReader.getNumericRef(i).toFloat() : inputReader.getFloatRef(i);
                    }
                    else if (types[i] == Type::Float64)
                    {
                        allocateColumn(double, double, 0);
                        columnValues[i]->value_double[row] = columns.getColumnType(i).isNumeric() ?
                            inputReader.getNumericRef(i).toFloat() : inputReader.getFloatRef(i);
                    }
                    else if (types[i] == Type::String)
                    {
                        allocateColumn(string, string, string());
                        columnValues[i]->value_string[row] = inputReader.getStringRef(i).str();
                    }
                    else if (types[i] == Type::Date)
                    {
                        allocateColumn(uint16_t, uint16, 0);
                        time_t t = getUnixTimeFromDate(inputReader.getDateRef(i));
                        columnValues[i]->value_uint16[row] = t / std::time_t(86400);
                    }
                    else if (types[i] == Type::DateTime)
                    {
                        allocateColumn(uint32_t, uint32, 0);
                        time_t t = getUnixTimeFromTimestamp(inputReader.getTimestampRef(i));
                        columnValues[i]->value_uint32[row] = t;
                    }
                }

                total++;
                row++;
                if (row == blockSize)
                {
                    writeBlock(tableName, names, types, columnValues);
                    row = 0;
                }

                if (pipelined)
                {
                    outputWriter.next();
                }
            }
            while (inputReader.next());

            if (row != 0)
            {
                for (size_t i = 0; i < columnValues.size(); ++i)
                {
                    if (!columnValues[i]) continue;
                    if (columnValues[i]->value_uint8.size()) columnValues[i]->value_uint8.resize(row);
                    if (columnValues[i]->value_uint16.size()) columnValues[i]->value_uint16.resize(row);
                    if (columnValues[i]->value_uint32.size()) columnValues[i]->value_uint32.resize(row);
                    if (columnValues[i]->value_uint64.size()) columnValues[i]->value_uint64.resize(row);
                    if (columnValues[i]->value_int8.size()) columnValues[i]->value_int8.resize(row);
                    if (columnValues[i]->value_int16.size()) columnValues[i]->value_int16.resize(row);
                    if (columnValues[i]->value_int32.size()) columnValues[i]->value_int32.resize(row);
                    if (columnValues[i]->value_int64.size()) columnValues[i]->value_int64.resize(row);
                    if (columnValues[i]->value_float.size()) columnValues[i]->value_float.resize(row);
                    if (columnValues[i]->value_double.size()) columnValues[i]->value_double.resize(row);
                    if (columnValues[i]->value_string.size()) columnValues[i]->value_string.resize(row);
                }
                writeBlock(tableName, names, types, columnValues);
            }

            if (!pipelined)
            {
                outputWriter.setInt(0, total);
                outputWriter.next();
            }

            srvInterface.log("export completed for %s", tableName.c_str());
        }
        catch(std::exception& e)
        {
            vt_report_error(0, "Exception while processing partition: [%s]", e.what());
        }
    }

    void writeBlock(std::string tableName, std::vector<string> names,
                    std::vector<int> types, std::vector<ColumnData*> &columnValues)
    {
        Block block;

        for (size_t i = 0; i < columnValues.size(); ++i)
        {
            if (!columnValues[i])
            {
                continue;
            }

            if (types[i] == Type::UInt8)
            {
                Column *col = new ColumnUInt8(std::move(columnValues[i]->value_uint8));
                block.AppendColumn(names[i], shared_ptr<Column>(col));
            }
            else if (types[i] == Type::UInt16)
            {
                Column *col = new ColumnUInt16(std::move(columnValues[i]->value_uint16));
                block.AppendColumn(names[i], shared_ptr<Column>(col));
            }
            else if (types[i] == Type::UInt32)
            {
                Column *col = new ColumnUInt32(std::move(columnValues[i]->value_uint32));
                block.AppendColumn(names[i], shared_ptr<Column>(col));
            }
            else if (types[i] == Type::UInt64)
            {
                Column *col = new ColumnUInt64(std::move(columnValues[i]->value_uint64));
                block.AppendColumn(names[i], shared_ptr<Column>(col));
            }
            else if (types[i] == Type::Int8)
            {
                Column *col = new ColumnInt8(std::move(columnValues[i]->value_int8));
                block.AppendColumn(names[i], shared_ptr<Column>(col));
            }
            else if (types[i] == Type::Int16)
            {
                Column *col = new ColumnInt16(std::move(columnValues[i]->value_int16));
                block.AppendColumn(names[i], shared_ptr<Column>(col));
            }
            else if (types[i] == Type::Int32)
            {
                Column *col = new ColumnInt32(std::move(columnValues[i]->value_int32));
                block.AppendColumn(names[i], shared_ptr<Column>(col));
            }
            else if (types[i] == Type::Int64)
            {
                Column *col = new ColumnInt64(std::move(columnValues[i]->value_int64));
                block.AppendColumn(names[i], shared_ptr<Column>(col));
            }
            else if (types[i] == Type::Float32)
            {
                Column *col = new ColumnFloat32(std::move(columnValues[i]->value_float));
                block.AppendColumn(names[i], shared_ptr<Column>(col));
            }
            else if (types[i] == Type::Float64)
            {
                Column *col = new ColumnFloat64(std::move(columnValues[i]->value_double));
                block.AppendColumn(names[i], shared_ptr<Column>(col));
            }
            else if (types[i] == Type::String)
            {
                Column *col = new ColumnString(columnValues[i]->value_string);
                block.AppendColumn(names[i], shared_ptr<Column>(col));
            }
            else if (types[i] == Type::Date)
            {
                ColumnUInt16 *colInt = new ColumnUInt16(std::move(columnValues[i]->value_uint16));
                ColumnDate *col = new ColumnDate();
                access_private::data_(*col) = shared_ptr<ColumnUInt16>(colInt);
                block.AppendColumn(names[i], shared_ptr<Column>(col));
            }
            else if (types[i] == Type::DateTime)
            {
                ColumnUInt32 *colInt = new ColumnUInt32(std::move(columnValues[i]->value_uint32));
                ColumnDateTime *col = new ColumnDateTime();
                access_private::data_(*col) = shared_ptr<ColumnUInt32>(colInt);
                block.AppendColumn(names[i], shared_ptr<Column>(col));
            }

            delete columnValues[i];
            columnValues[i] = NULL;
        }

        client->Insert(tableName, block);
    }
};


class ClickhouseExportFactory : public TransformFunctionFactory
{

    virtual void getParameterType(ServerInterface &srvInterface, SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addVarchar(256, "relation");
        parameterTypes.addBool("ddl");

        parameterTypes.addVarchar(1024, "host");
        parameterTypes.addInt("port");
        parameterTypes.addVarchar(256, "database");
        parameterTypes.addVarchar(256, "user");
        parameterTypes.addVarchar(256, "password");

        parameterTypes.addVarchar(256, "dsn");
        parameterTypes.addVarchar(256, "dsn_path");
    }

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType)
    {
        argTypes.addAny();
        returnType.addInt();
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &inputTypes,
                               SizedColumnTypes &outputTypes)
    {
        outputTypes.addInt("exported_rows");
    }

    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
    {
        if (!srvInterface.getParamReader().containsParameter("relation"))
        {
            vt_report_error(0, "relation name is required");
        }

        return vt_createFuncObj(srvInterface.allocator, ClickhouseExport, false);
    }

};

RegisterFactory(ClickhouseExportFactory);


class ClickhousePipeFactory : public TransformFunctionFactory
{

    virtual void getParameterType(ServerInterface &srvInterface, SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addVarchar(256, "relation");
        parameterTypes.addBool("ddl");

        parameterTypes.addVarchar(1024, "host");
        parameterTypes.addInt("port");
        parameterTypes.addVarchar(256, "database");
        parameterTypes.addVarchar(256, "user");
        parameterTypes.addVarchar(256, "password");

        parameterTypes.addVarchar(256, "dsn");
        parameterTypes.addVarchar(256, "dsn_path");
    }

    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType)
    {
        argTypes.addAny();
        returnType.addAny();
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &inputTypes,
                               SizedColumnTypes &outputTypes)
    {
        for (size_t i = 0; i < inputTypes.getColumnCount(); i++)
        {
            outputTypes.addArg(inputTypes.getColumnType(i), inputTypes.getColumnName(i));
        }
    }

    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
    {
        if (!srvInterface.getParamReader().containsParameter("relation"))
        {
            vt_report_error(0, "relation name is required");
        }

        return vt_createFuncObj(srvInterface.allocator, ClickhouseExport, true);
    }

};

RegisterFactory(ClickhousePipeFactory);
