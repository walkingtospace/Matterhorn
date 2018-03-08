package ecs;

// Java Import
import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.Integer;
import java.lang.Process;
import java.lang.Runtime;
import java.lang.String;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Random;

// Exception
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

// Internal Import
import common.helper.MD5Hasher;
import common.message.MetaData;

// ZooKeeper Import
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;


public class ECS implements Watcher{

    private ArrayList<IECSNode> availServers;
    private ArrayList<IECSNode> usedServer;
    private ArrayList<HashRingEntry> hashRing;
    private Map<String, IECSNode> escnMap;
    private MD5Hasher hasher;
    private MetaData metaData;
    private static final String zkHost = "0.0.0.0";
    private static final int zkPort = 2083;
    private ZooKeeper zk;

    // Testing only, need to remove main!
    public static void main() {
    	ECS ecs = new ECS("");
    }

    
    public ECS(String configPath) {
        // Init the class variable
        availServers = new ArrayList<IECSNode>();
        usedServer = new ArrayList<IECSNode>();
        hashRing = new ArrayList<HashRingEntry>();
        metaData = new MetaData();
        try {
			hasher = new MD5Hasher();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

        // Read the content of the configuration file
        boolean status = this.parseConfig(configPath);

        // Connect to ZK server
        try {
			status = this.connectToZK();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }


    public boolean start() {
        boolean status;
        boolean global_status = true;

        for (IECSNode esc: availServers) {
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

        for (IECSNode esc: availServers) {
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

        for (IECSNode esc: availServers) {
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
        IECSNode availServer = this.randomlyPickOneAvailServer();
        
        if (availServer == null) {
            System.out.print("No server is available");
            return null;
        }

        status = this.markServerUnavail(availServer);

        // Create the Znode in ZooKeeper
        status = this.createZnode(availServer, cacheStrategy, cacheSize);

        // Add the ECSNode to the hashring
        status = this.addECSNodeToHashRing(availServer);

        // Configure the hash range of the node
        status = this.recalculateHashRange();

        // Trigger the start of server
        status = this.runServerOnShell();
        if (status == false) {
            System.out.println("Failed to start server on shell");
            return null;
        }

        return availServer;
    }


    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        ArrayList<IECSNode> res = new ArrayList<IECSNode>();
 
        if (availServers.size() < count) {
            System.out.println("Not enough free servers");
            return res;
        }

        int i = count;
        IECSNode new_node;
        while(i > 0) {
            new_node = addNode(cacheStrategy, cacheSize);
            if (new_node != null) {
                res.add(new_node);
            } else {
                System.out.println("Something is wrong. Node not added to addNodes");
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
        return escnMap.get(Key);
    }

    public void process(WatchedEvent event) {
        return;
    }

    private boolean connectToZK() throws KeeperException, IOException{
        // Use Zookeeper
    	this.zk = new ZooKeeper(this.zkHost, this.zkPort, this);
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
                ECSNode sc = new ECSNode(tokens[0],
                                         tokens[1],
                                         Integer.parseInt(tokens[2]),
                                         new BigInteger("-1"),
                                         new BigInteger("-1"),
                                         new BigInteger("-1"));
                this.escnMap.put(tokens[0], sc);
                this.availServers.add(sc);
            }

            // Always close files.
            bufferedReader.close();
            return true;
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
          proc = run.exec(command);
          return true;
        } catch (IOException e) {
          e.printStackTrace();
          return false;
        }
    }


    private IECSNode randomlyPickOneAvailServer() {
        Random rand = new Random();
        int arr_size = this.availServers.size();
        if (arr_size == 0) {
            return null;
        }
        int i = rand.nextInt(arr_size);

        return this.availServers.get(i);
    }


    private boolean markServerUnavail(IECSNode escn) {
        // add escn to usedServer
        this.usedServer.add(escn);
        // remove escb from availServer
        this.availServers.remove(escn);

        return true;
    }


    private boolean markServeravail(IECSNode escn) {
        // add escn to usedServer
        this.availServers.add(escn);
        // remove escb from availServer
        this.usedServer.remove(escn);
        
        return true;
    }


    private boolean createZnode(IECSNode escn, String cacheStrategy, int cacheSize) {
        // use Zookeeper
    	return false;
    }


    private boolean addECSNodeToHashRing(IECSNode escn) {
        // Hash the server's name
        BigInteger nameHash = hasher.hashString(escn.getNodeName());
        HashRingEntry ringEntry = new HashRingEntry(escn, nameHash);

        // Insert the ringEntry into the hash ring that preserves order
        if (this.hashRing.size() == 0) {
            this.hashRing.add(ringEntry);
        } else {
            // Find the index of the position
            int i = 0;
            while(i < this.hashRing.size()) {
                if (this.hashRing.get(i).hashValue.compareTo(nameHash) == -1) {
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
        boolean status = true;
        ECSNode escn;

        // if there's only one entry
        if (numRingEntry == 1) {
            ringEntry = this.hashRing.get(0);
            escn = (ECSNode)ringEntry.escn;
            escn.leftHash = new BigInteger("0"); // Minimal Hash
            escn.rightHash = new BigInteger("FFFFFF"); // Biggest Hash
            status = updateZnode(ringEntry.escn);
        } else {
            while(i < numRingEntry) {
                ringEntry = this.hashRing.get(0);
                escn = (ECSNode)ringEntry.escn;
                if(i == 0) {
                	escn.leftHash = hashRing.get(numRingEntry - 1).hashValue;
                	escn.rightHash = ringEntry.hashValue;
                } else {
                    // Last entry
                	escn.leftHash = hashRing.get(i - 1).hashValue;
                    escn.rightHash = hashRing.get(i).hashValue;
                }
                status = updateZnode(ringEntry.escn);
                i++;
            }
        }
        return status;
    }

    private boolean updateZnode(IECSNode escn) {
        // update znode
    	return false;
    }

    private boolean startServer(IECSNode escn) {
        // Change the state of node in Znode to start
    	return false;
    }


    private boolean stopServer(IECSNode escn) {
        // Change the state of node in Znode to stop
    	return false;
    }


    private boolean shutdownServer(IECSNode escn) {
        // Use SSH to shutdown and kill the node
    	return false;
    }
}