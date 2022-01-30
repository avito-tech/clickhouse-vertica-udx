
#ifndef _UTILS_H
#define _UTILS_H

#include <unordered_map>
#include <vector>
#include <string>

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)

#ifndef DSN_SELECT_DEFAULT
#   error No DSN_SELECT_DEFAULT is provided! Add -DDSN_SELECT_DEFAULT=<DSN> to compilation flags
#else
#   define DSN_SELECT_DEFAULT_STR TOSTRING(DSN_SELECT_DEFAULT)
#endif

#ifndef DSN_INSERT_DEFAULT
#   error No DSN_INSERT_DEFAULT is provided! Add -DDSN_INSERT_DEFAULT=<DSN> to compilation flags
#else
#   define DSN_INSERT_DEFAULT_STR TOSTRING(DSN_INSERT_DEFAULT)
#endif

#ifndef DSN_PATH_DEFAULT
#   error No DSN_PATH_DEFAULT is provided! Add -DSN_PATH_DEFAULT=<DSN_PATH> to compilation flags
#else
#   define DSN_PATH_DEFAULT_STR TOSTRING(DSN_PATH_DEFAULT)
#endif


int getTZLocalShift();

std::string toLower(const std::string &s);

std::vector<std::string> splitString(std::string s, char delim);

std::vector<std::string> splitPair(std::string s, char delim);

std::string replaceSubstring(std::string s, std::string pattern, std::string repl);

std::string getSessionId(std::string nodeName);

bool isCaption(std::string token);

std::unordered_map<std::string, std::string> parseToml(std::string dsnPath, std::string dsn);

float exp_compat(float x, int n = 100);

namespace Vertica {

    class ServerInterface;
    class ParamReader;
}

std::string getSessionId(Vertica::ServerInterface &srvInterface);

std::unordered_map<std::string, std::string> getParams(const Vertica::ParamReader &params, const char * dsnDefault);

namespace clickhouse {

    class ClientOptions;
    class Client;
}

clickhouse::Client *
createClickhouseClient(clickhouse::ClientOptions &opt,
                       std::vector<std::string> hostAlt,
                       Vertica::ServerInterface &srvInterface);

#endif
