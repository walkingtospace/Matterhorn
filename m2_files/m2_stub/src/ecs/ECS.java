package ecs;

// Java Import
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.Integer;
import java.lang.Process
import java.lang.Runtime;
import java.lang.String;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Random;

// Internal Import
import common.helper.MD5Hasher;
import common.message.MetaData;

// ZooKeeper Import
import org.apache.zookeeper.ZooKeeper;


public class ECS {

    private ArrayList<IESCNode> availServers;
    private ArrayList<IESCNode> usedServer;
    private ArrayList<HashRingEntry> hashRing;
    private Map<String, IECSNode> escnMap;
    private MD5Hasher hasher;
    private MetaData metaData;
    private static final String zkHost = "0.0.0.0";
    private static final int zkPort = 2083;

    public ECS(String configPath) {
        // Init the class variable
        availServers = new ArrayList<IESCNode>();
        usedServer = new ArrayList<IESCNode>();
        hashRing = new ArrayList<HashRingEntry>();
        escnMap = new Map<String, IECSNode>();
        hasher = new MD5Hasher();
        metaData = new MetaData();

        // Read the content of the configuration file
        boolean status = this.parseConfig(configPath);

        // Connect to ZK server
        boolean status = this.connectToZK();
    }


    public boolean start() {
        boolean status;
        boolean global_status = true;

        for (ESCNode esc: availServers) {
            status = startServer(esc);
            if (status == false) {
                global_status = false;
                System.out.println("Failed to start server");
            }
        }

        return global_status;
    }


    public boolean stop() {
        boolean status;
        boolean global_status = true;

        for (ESCNode esc: availServers) {
            status = stopServer(esc);
            if (status == false) {
                global_status = false;
                System.out.println("Failed to start server");
            }
        }

        return global_status;
    }


    public boolean shutdown() {
        boolean status;
        boolean global_status = true;

        for (ESCNode esc: availServers) {
            status = shutdownServer(esc);
            if (status == false) {
                global_status = false;
                System.out.println("Failed to start server");
            }
        }

        return global_status;
    }


    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        boolean status;
        // Randomly pick one available server from a list of available servers
        ESCNode availServer = this.randomlyPickOneAvailServer();
        
        if (availServer == null) {
            System.out.print("No server is available");
            return null;
        }

        status = this.markServerUnavail(availServer);

        // Create the Znode in ZooKeeper
        status = this.createZnode(availServer, cacheStrategy, cacheSize);

        // Add the ECSNode to the hashring
        boolean status = this.addECSNodeToHashRing(escn);

        // Configure the hash range of the node
        boolean status = this.recalculateHashRange();

        // Trigger the start of server
        boolean status = this.runServerOnShell();
        if (status == false) {
            System.out.println("Failed to start server on shell");
            return null;
        }

        return escn;
    }


    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        Collection<IECSNode> res = new Collection<IECSNode>();

        if (availServers.size() < count) {
            System.out.println("Not enough free servers");
            return res;
        }

        int i = count;
        IECSNode new_node;
        while(i > 0) {
            new_node = addNode(cacheStrategy, cacheSize)
            if (new_node != null) {
                res.add(new_node);
            } else {
                System.println("Something is wrong. Node not added to addNodes");
            }
            i--;
        }

        return res;
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
        return escnMap;
    }


    public IECSNode getNodeByKey(String Key) {
        return escnMap[key];
    }


    private boolean connectToZK() {
        // Use Zookeeper
        return false;
    }


    private BigInteger hash_server(String ip, int port) {
        String portString = String.valueOf(port);
        String hash_input = ip + ":" + port;

        return hasher.hashString(hash_input);
    }


    private boolean parseConfig(String configPath) {
        // This will reference one line at a time
        String line = null;

        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = 
                new FileReader(configPath);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = 
                new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                String[] tokens = line.split("\\s+");
                // Each line should look like this server1 127.0.0.1 50000
                ESCNode sc = new ESCNode(tokens[0],
                                         tokens[1],
                                         Integer.parseInt(tokens[2]),
                                         -1,
                                         -1);
                this.escnMap[tokens[0]] = sc;
                this.availServers.push(sc);
            }

            // Always close files.
            bufferedReader.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                "Unable to open file '" + 
                configPath + "'");
            return false;
        }
        catch(IOException ex) {
            System.out.println(
                "Error reading file '" 
                + configPath + "'");
            return false;
        }
    }


    private boolean runServerOnShell() {
        Process proc;
        String command = "ssh -n <username>@localhost nohup java -jar <path>/ms2-server.jar 50000 ERROR &";
        Runtime run = Runtime.getRuntime();
        try {
          proc = run.exec(script);
        } catch (IOException e) {
          e.printStackTrace();
        }
    }


    private ESCNode randomlyPickOneAvailServer() {
        Random rand = new Random();
        int arr_size = this.availServers.size();
        if (arr_size == 0) {
            return null;
        }

        int i = rand.nextInt(arr_size);

        return this.availServers[i];
    }


    private boolean markServerUnavail(ESCNode escn) {
        // add escn to usedServer
        this.usedServer.push(escn);
        // remove escb from availServer
        this.availServers.remove(escn);
    }



    private boolean markServeravail(ESCNode escn) {
        // add escn to usedServer
        this.availServers.push(escn);
        // remove escb from availServer
        this.usedServer.remove(escn);
    }


    private boolean createZnode(ESCNode escn) {
        // use Zookeeper
    }


    private boolean addECSNodeToHashRing(IECSNode escn) {
        // Hash the server's name
        BigInteger nameHash = hasher.hashString(escn);
        HashRingEntry ringEntry = new HashRingEntry(escn, nameHash);

        // Insert the ringEntry into the hash ring that preserves order
        if (this.hashRing.size() == 0) {
            this.hashRing.push(ringEntry);
        } else {
            // Find the index of the position
            int i = 0;
            while(i < this.hashRing.size()) {
                if (this.hashRing[i].hashValue >= nameHash) {
                    break;
                }
                i++;
            }
            this.hashRing.add(i, ringEntry);
        }

        // recalculate the hash range
        return true;
    }

    private boolean recalculateHashRange() {
        // find all escn entry in the Hash Ring
        int numRingEntry = this.hashRing.size();
        int i = 0;
        HashRingEntry ringEntry;
        boolean status;

        // if there's only one entry
        if (numRingEntry == 1) {
            ringEntry = this.hashRing[0]
            ringEntry.escn.setLeftHash(""); // Minimal Hash
            ringEntry.escn.setRightHash(""); // Biggest Hash
            status = updateZnode(ringEntry.escn);
        } else {
            while(i < numRingEntry) {
                ringEntry = this.hashRing[i];
                if(i == 0) {
                    ringEntry.escn.setLeftHash(hashRing[numRingEntry - 1].hashValue)
                    ringEntry.escn.setRightHash(ringEntry.hashValue);
                } else {
                    // Last entry
                    ringEntry.escn.setLeftHash(hashRing[i - 1].hashValue);
                    ringEntry.escn.setRightHash(hashRing[i].hashValue);
                }
                status = updateZnode(ringEntry.escn);
                i++;
            }
        }
    }

    private boolean updateZnode(IECSNode escn) {
        // update znode
    }

    private boolean startServer(IECSNode escn) {
        // Change the state of node in Znode to start
    }


    private boolean stopServer(IECSNode escn) {
        // Change the state of node in Znode to stop
    }


    private boolean shutdownServer(IECSNode escn) {
        // Use SSH to shutdown and kill the node 
    }
}