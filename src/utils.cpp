#include "utils.h"
#include "Vertica.h"
#include "clickhouse/client.h"

#include <iostream>
#include <ctime>
#include <regex>
#include <unistd.h>
#include <fstream>

using namespace Vertica;
using namespace std;

float exp_compat(float x, int n)
{
    float sum = 1.0f;

    for (int i = n - 1; i > 0; --i )
        sum = 1 + x * sum / i;

    return sum;
}

int getTZLocalShift()
{
    time_t rawtime = time(0);
    return timegm(localtime(&rawtime)) - rawtime;
}

string toLower(const string &s)
{
    string c(s);
    transform(c.begin(), c.end(), c.begin(), ::tolower);
    return c;
}

vector<string> splitString(string s, char delim)
{
    vector<string> res;
    istringstream f(s);
    string p;
    while (getline(f, p, delim))
    {
        res.push_back(p);
    }
    return res;
}

vector<string> splitPair(string s, char delim)
{
    vector<string> res;
    auto pos = s.find(delim);
    if (pos == string::npos)
    {
        res.push_back(s); 
    }
    else
    {
        res.push_back(s.substr(0, pos));
        res.push_back(s.substr(pos + 1));
    }
    return res;
}

string replaceSubstring(string s, string pattern, string repl)
{
    regex regexp(pattern);
    smatch m;
    if (regex_search(s, m, regexp))
    {
        s = m.prefix().str() + " " + repl + " " + m.suffix().str();
    }
    return s;
}

string getSessionId(string nodeName)
{
    char buf[1024];
    snprintf(buf, 1024, "/proc/%d/cmdline", getpid());

    FILE *fp = fopen(buf, "r");
    if (fp == NULL) 
    {
        return string();
    }

    size_t size = fread(buf, 1, 1024, fp);
    fclose(fp);

    size_t start = 0;
    size_t len = 0;

    for (size_t i = 0; i < size; ++i)
    {
        if (buf[i] >= 0x20) 
        {
            continue;
        }

        len = i - start;
        if (len > nodeName.size() && string(buf + start, nodeName.size()) == nodeName)
        {
            return string(buf + start, len);
        }
        start = i + 1;
    }

    return string();   
}

string getSessionId(ServerInterface &srvInterface)
{
    string node = srvInterface.getCurrentNodeName();
    return getSessionId(node.substr(0, node.size() - 4));
}

bool isCaption(string token) {
    return token.size() > 2 && token[0] == '[' && token[token.size() - 1] == ']';
}

unordered_map<string, string>
parseToml(string dsnPath, string dsn)
{
    unordered_map<string, string> result;
    ifstream dsnFile(dsnPath);

    if (!dsnFile.is_open())
    {
        return result;
    }

    stringstream ss;
    ss << '[' << dsn << ']';
    string dsnCaption = ss.str();

    string currentLine;
    bool isDsnCaption = false;

    while (getline(dsnFile, currentLine))
    {
        if (isCaption(currentLine))
        {
            isDsnCaption = currentLine == dsnCaption;
        }

        if (isDsnCaption)
        {
            auto keyvalue = splitPair(currentLine, '=');
            if (keyvalue.size() != 2) {
                continue;
            }
            auto key = keyvalue[0];
            auto value = keyvalue[1];

            auto unquotedValue = splitString(value, '\'');

            if (unquotedValue.size() >= 2)
            {
                result[key] = unquotedValue[1];
            }
            else
            {
                result[key] = value;
            }
        }
    }
    return result;
}

unordered_map<string, string> getParams(const ParamReader &params, const char * dsnDefault)
{
    unordered_map<string, string> result;

    result["dsn_path"] = DSN_PATH_DEFAULT_STR;
    if (params.containsParameter("dsn_path"))
    {
        result["dsn_path"] = params.getStringRef("dsn_path").str();
    }

    result["dsn"] = dsnDefault;
    if (params.containsParameter("dsn"))
    {
        result["dsn"]  = params.getStringRef("dsn").str();
    }

    auto tomlParams = parseToml(result["dsn_path"], result["dsn"]);
    result.insert(tomlParams.begin(), tomlParams.end());

    for (string key : params.getParamNames())
    {
        if (params.getType(key).isStringType())
        {
            result[key] = params.getStringRef(key).str();
        }
    }

    return result;
}

clickhouse::Client *
createClickhouseClient(clickhouse::ClientOptions &opt,
                       std::vector<std::string> hostAlt,
                       Vertica::ServerInterface &srvInterface)
{
    for (string host : hostAlt)
    {
        opt.SetHost(host);
        try
        {
            return new clickhouse::Client(opt);
        }
        catch(exception &e)
        {
            srvInterface.log("failed to connect to %s: %s", host.c_str(), e.what());
        }
    }
    return NULL;
}
