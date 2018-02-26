package ecs;

// Java Import
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

// Internal Import
import common.messages.MetaData;

public class ECS {

    private ArrayList<ServerConfig> availServers;
    private ArrayList<ServerConfig> usedServer;
    private MetaData metaData;
    private String zkHost;
    private int zkPort;

    public ECS(String config_path) {
        // Read the content of the configuration file
    }


    public boolean start() {
        // TODO
        return false;
    }


    public boolean stop() {
        // TODO
        return false;
    }


    public boolean shutdown() {
        // TODO
        return false;
    }


    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }


    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }


    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }


    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }


    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }


    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }


    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }

}