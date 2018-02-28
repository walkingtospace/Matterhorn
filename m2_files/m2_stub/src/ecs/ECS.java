package ecs;

// Java Import
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.math.BigInteger;
import java.lang.String;

// Internal Import
import common.helper.MD5Hasher;
import common.message.MetaData;

// ZooKeeper Import
import org.apache.zookeeper.ZooKeeper;


public class ECS {

    private ArrayList<ServerConfig> availServers;
    private ArrayList<ServerConfig> usedServer;
    private ArrayList<ECSNode> hashRing;
    private MD5Hasher hasher;
    private MetaData metaData;
    private static final String zkHost = "0.0.0.0";
    private static final int zkPort = 2083;

    public ECS(String config_path) {
        // Init the class variable
        availServers = new ArrayList<ServerConfig>();
        usedServer = new ArrayList<ServerConfig>();
        hashRing = new ArrayList<ECSNode>();
        hasher = new MD5Hasher();
        metaData = new MetaData();

        // Read the content of the configuration file
        

        // Connect to ZK server
        boolean status = this.connectToZK();
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


    private boolean connectToZK() {
        return false;
    }


    private IECSNode createECSNodeFromServerConfig(ServerConfig sc) {
        // Calculate left and right hash of the server
        BigInteger leftHash = BigInteger.ZERO;
        BigInteger rightHash = BigInteger.ONE;

        // Populate the other attributes
        ECSNode escn = new ECSNode(sc.getName(), sc.getIP(), sc.getPort(), leftHash, rightHash);

        return escn;
    }


    private BigInteger hash_server(String ip, int port) {
        String portString = String.valueOf(port);
        String hash_input = ip + ":" + port;
        return hasher.hashString(hash_input);
    }
}