#include "Vertica.h"


using namespace Vertica;
using namespace std;


class VoidSource : public UDSource {

    StreamState process(ServerInterface &srvInterface, DataBuffer &output) 
    {
        if (output.size < 1)
        {
            return OUTPUT_NEEDED;
        }
        output.offset = 1;
        return DONE;
    }
};

class VoidSourceFactory : public SourceFactory {
public:

    void plan(ServerInterface &srvInterface,
            NodeSpecifyingPlanContext &planCtxt) 
    {
        ParamReader params = srvInterface.getParamReader();
        string nodes = (params.containsParameter("nodes") ?
                        params.getStringRef("nodes").str() : string("all"));

        if (nodes == string("initiator"))
        {
            vector<string> nodes;
            nodes.push_back(srvInterface.getCurrentNodeName());
            planCtxt.setTargetNodes(nodes);
        }
        else //if (nodes == string("all"))
        {
            planCtxt.setTargetNodes(planCtxt.getClusterNodes());
        }
    }

    std::vector<UDSource*> prepareUDSources(ServerInterface &srvInterface,
            NodeSpecifyingPlanContext &planCtxt) 
    {
        ParamReader params = srvInterface.getParamReader();
        int threadCount = (params.containsParameter("thread_count") ? 
                           params.getIntRef("thread_count") : 1);

        std::vector<UDSource*> retVal;
        for(int i = 0; i < threadCount; i++)
        {
            retVal.push_back(vt_createFuncObject<VoidSource>(srvInterface.allocator));
        }
        return retVal;
    }
    void getParameterType(ServerInterface &srvInterface, SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addInt("thread_count");
        parameterTypes.addVarchar(1024, "nodes");
    }

};

RegisterFactory(VoidSourceFactory);
