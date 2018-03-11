
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
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

// Exception
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

// Internal Import
import common.helper.MD5Hasher;

// JSON
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

// ZooKeeper Import
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;


public class ECS implements Watcher{

    public ArrayList<IECSNode> availServers;
    public ArrayList<IECSNode> usedServers;
    public ArrayList<HashRingEntry> hashRing;
    public HashMap<String, IECSNode> escnMap;
    public MD5Hasher hasher;
    public static final String zkHost = "0.0.0.0";
    public static final int zkPort = 3200;
    public ZooKeeper zk;

    
    public ECS(String configPath) {
        // Init the class variable
        this.availServers = new ArrayList<IECSNode>();
        this.usedServers = new ArrayList<IECSNode>();
        this.hashRing = new ArrayList<HashRingEntry>();
        this.escnMap = new HashMap<String, IECSNode>();
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
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        // Need to remove
        // this.test();
    }

    // Purely testing purpose. Should remove after everything is done
    public void test() {
    	// Test Create Znode
    	IECSNode n0 = this.addNode("FIFO", 1024);
    	IECSNode n1 = this.addNode("FIFO", 1024);
    	System.out.println(n0);
    	System.out.println(n1);
    	System.out.println("Hash Ring:");
    	for (HashRingEntry r: this.hashRing) {
    		System.out.println(r);
    	}
    }

    public boolean start() {
        boolean status;
        boolean global_status = true;

        for (IECSNode esc: this.usedServers) {
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

        for (IECSNode esc: this.usedServers) {
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

        for (IECSNode esc: this.usedServers) {
            status = shutdownServer(esc);
            if (status == false) {
                global_status = false;
                System.out.println("Failed to shutdown server");
            }
        }

        // Return the usedServers to avaiServers
        this.availServers.addAll(this.usedServers);
        this.usedServers.clear();
        return global_status;
    }


    public IECSNode addNode(String cacheStrategy, int cacheSize) {
    	Collection<IECSNode> res = this.addNodes(1, cacheStrategy, cacheSize);
        return ((ArrayList<IECSNode>)res).get(0);
    }


    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
    	Collection<IECSNode> res = this.setupNodes(count, cacheStrategy, cacheSize);
 
        return res;
    }

    public IECSNode setupNode(String cacheStrategy, int cacheSize) {
    	// Create a new KVServer with the specified cache size and replacement strategy
    	// and add it to the storage service at an arbitrary position.
        boolean status;
        
        // Randomly pick one available server from a list of available servers
        IECSNode availServer = this.randomlyPickOneAvailServer();
        
        if (availServer == null) {
            System.out.print("No server is available");
            return null;
        }

        status = this.markServerUnavail(availServer);

        // Create Znode on ZK
        status = this.createZnode(availServer, cacheStrategy, cacheSize);

        // SSH start server
        status = this.sshStartServer(availServer);
        
        // Wait until it is added
        try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        
        // Add the ECSNode to the hashring
        status = this.addECSNodeToHashRing(availServer);

        // Configure the hash range of the node
        status = this.recalculateHashRange();

        // Wait until the transfer is successful
        status = this.waitTransfer();

        return availServer;
    }
    
    public boolean waitTransfer() {
    	try {
    		System.out.println("Waiting transfer to be done");
			Thread.sleep(1 * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
    	return true;
    }
 
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // Create IECSNodes for the servers, but do not issue the ssh call to start the processes. Should be invoked by addNodes
        ArrayList<IECSNode> res = new ArrayList<IECSNode>();
        
        if (availServers.size() < count) {
            System.out.println("Not enough free servers");
            return res;
        }

        int i = count;
        IECSNode new_node;
        while(i > 0) {
            new_node = setupNode(cacheStrategy, cacheSize);
            if (new_node != null) {
                res.add(new_node);
            } else {
                System.out.println("Something is wrong. Node not added to addNodes");
            }
            i--;
        }

        return res;
    }


    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }


    public boolean removeNode(IECSNode ecsn) {
    	this.removeESCNodeFromHashRing(ecsn);
    	this.recalculateHashRange();
    	return true;
    }
 
    public boolean removeNodes(Collection<String> nodeNames) {
        // Remove nodes
    	IECSNode escn;
    	for (String nodeName: nodeNames) {
    		escn = getNodeByKey(nodeName);
    		this.removeNode(escn);
    	}
        return true;
    }


    public Map<String, IECSNode> getNodes() {
        return escnMap;
    }


    public IECSNode getNodeByKey(String Key) {
        return escnMap.get(Key);
    }

    public void process(WatchedEvent event) {
    	//System.out.println("Trigger Event from ZK");
        return;
    }

    public void printHashRing() {
    	// Print the Hash Ring
    	System.out.println("The Hash Ring:");
    	for (HashRingEntry r: this.hashRing) {
    		System.out.println(r);
    	}
    }

    public void printServerList() {
    	// Print Available Servers
    	System.out.println("Available Servers:");
    	for (IECSNode e: this.availServers) {
    		System.out.println(e.getNodeName());
    	}
    	// Print Used Servers
    	System.out.println("Used Servers");
    	for (IECSNode e: this.usedServers) {
    		System.out.println(e.getNodeName());
    	}
    }
    
    public boolean connectToZK() throws KeeperException, IOException{
        // Use Zookeeper
    	String connection = this.zkHost + ":" + Integer.toString(this.zkPort);
    	this.zk = new ZooKeeper(connection, 3000, this);
        return true;
    }

    public boolean parseConfig(String configPath) {
        // This will reference one line at a time
        String line;

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
                                         "-1",
                                         "-1",
                                         "-1",
                                         "",
                                         -1,
                                         "STOP",
                                         "null",
                                         "OFF");
                try{
                	this.escnMap.put(tokens[0], sc);
                } catch(Exception e) {
        			e.printStackTrace();
                }
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


    public boolean sshStartServers(Collection<IECSNode> res) {
    	for (IECSNode escn: res) {
        	System.out.println("Running SSH to start" + escn.getNodeName());
//            Process proc;
//            String command = "ssh -n <username>@localhost nohup java -jar <path>/ms2-server.jar 50000 ERROR &";
//            Runtime run = Runtime.getRuntime();
    // 
//            try {
//              proc = run.exec(command);
//              return true;
//            } catch (IOException e) {
//              e.printStackTrace();
//              return false;
//            }	
    	}
    	return true;
    }

    public boolean sshStartServer(IECSNode res) {
        System.out.println("Running SSH to start" + res.getNodeName());
//            Process proc;
//            String command = "ssh -n <username>@localhost nohup java -jar <path>/ms2-server.jar 50000 ERROR &";
//            Runtime run = Runtime.getRuntime();
    // 
//            try {
//              proc = run.exec(command);
//              return true;
//            } catch (IOException e) {
//              e.printStackTrace();
//              return false;
//            }	
    	return true;
    }


    public IECSNode randomlyPickOneAvailServer() {
        Random rand = new Random();
        int arr_size = this.availServers.size();
        if (arr_size == 0) {
            return null;
        }
        int i = rand.nextInt(arr_size);

        return this.availServers.get(i);
    }


    public boolean markServerUnavail(IECSNode escn) {
        // add escn to usedServer
        this.usedServers.add(escn);
        // remove escb from availServer
        this.availServers.remove(escn);

        return true;
    }


    public boolean markServeravail(IECSNode escn) {
        // add escn to usedServer
        this.availServers.add(escn);
        // remove escb from availServer
        this.usedServers.remove(escn);
        
        return true;
    }


    public boolean createZnode(IECSNode escn, String cacheStrategy, int cacheSize) {
        // Set attributes
    	((ECSNode)escn).cacheStrategy = cacheStrategy;
    	((ECSNode)escn).cacheSize = cacheSize;
    	
    	// Create Znode
    	String zkPath = "/" + escn.getNodeName();
    	JSONObject jsonMessage = new JSONObject();
        jsonMessage.put("NodeName", escn.getNodeName());
        jsonMessage.put("NodeHost", escn.getNodeHost());
        jsonMessage.put("NodePort", escn.getNodePort());
        jsonMessage.put("CacheStrategy", cacheStrategy);
        jsonMessage.put("CacheSize", cacheSize);
        jsonMessage.put("State", ((ECSNode)escn).state);
        jsonMessage.put("NodeHash", ((ECSNode)escn).nameHash);
        jsonMessage.put("LeftHash", ((ECSNode)escn).leftHash);
        jsonMessage.put("RightHash", ((ECSNode)escn).rightHash);
        jsonMessage.put("Target", ((ECSNode)escn).target);
        jsonMessage.put("Transfer", ((ECSNode)escn).transfer);
        byte[] zkData = jsonMessage.toString().getBytes();
        try {
			this.zk.create(zkPath, zkData, ZooDefs.Ids.OPEN_ACL_UNSAFE,
				      CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			e.printStackTrace();
			return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
        
    	return true;
    }
    
    public boolean removeZnode(IECSNode escn) {
    	String path = "/" + escn.getNodeName();
    	try {
			this.zk.delete(path,zk.exists(path,true).getVersion());
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		} catch (KeeperException e) {
			e.printStackTrace();
			return false;
		}
    	return true;
    }

    public boolean updateZnodeState(IECSNode escn, String state) {
        // update znode
//    	if (((ECSNode)escn).state == state) {
//    		// Don't reupdate
//    		return true;
//    	}
    	String zkPath = "/" + escn.getNodeName();
    	JSONObject jsonMessage = new JSONObject();
        jsonMessage.put("NodeName", escn.getNodeName());
        jsonMessage.put("NodeHost", escn.getNodeHost());
        jsonMessage.put("NodePort", escn.getNodePort());
        jsonMessage.put("CacheStrategy", ((ECSNode)escn).cacheStrategy);
        jsonMessage.put("CacheSize", ((ECSNode)escn).cacheSize);
        jsonMessage.put("State", state);
        jsonMessage.put("NodeHash", ((ECSNode)escn).nameHash);
        jsonMessage.put("LeftHash", ((ECSNode)escn).leftHash);
        jsonMessage.put("RightHash", ((ECSNode)escn).rightHash);
        jsonMessage.put("Target", ((ECSNode)escn).target);
        jsonMessage.put("Transfer", ((ECSNode)escn).transfer);
        byte[] zkData = jsonMessage.toString().getBytes();
        try {
			this.zk.setData(zkPath, zkData, this.zk.exists(zkPath,true).getVersion());
		} catch (KeeperException e) {
			e.printStackTrace();
			return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
        
    	return true;
    }

    public boolean updateZnodeHash(IECSNode escn, String leftHash, String rightHash) {
        // update znode
//    	if(((ECSNode)escn).leftHash == leftHash && ((ECSNode)escn).rightHash == rightHash) {
//    		return true;
//    	}
    	String zkPath = "/" + escn.getNodeName();
    	JSONObject jsonMessage = new JSONObject();
        jsonMessage.put("NodeName", escn.getNodeName());
        jsonMessage.put("NodeHost", escn.getNodeHost());
        jsonMessage.put("NodePort", escn.getNodePort());
        jsonMessage.put("CacheStrategy", ((ECSNode)escn).cacheStrategy);
        jsonMessage.put("CacheSize", ((ECSNode)escn).cacheSize);
        jsonMessage.put("State", ((ECSNode)escn).state);
        jsonMessage.put("NodeHash", ((ECSNode)escn).nameHash);
        jsonMessage.put("LeftHash", leftHash);
        jsonMessage.put("RightHash", rightHash);
        jsonMessage.put("Target", ((ECSNode)escn).target);
        jsonMessage.put("Transfer", ((ECSNode)escn).transfer);
        byte[] zkData = jsonMessage.toString().getBytes();
        try {
			this.zk.setData(zkPath, zkData, this.zk.exists(zkPath,true).getVersion());
		} catch (KeeperException e) {
			e.printStackTrace();
			return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
        
    	return true;
    }

    public boolean updateZnodeNodeHash(IECSNode escn, String nodeHash) {
        // update znode
//    	if(((ECSNode)escn).nameHash == nodeHash) {
//    		return true;
//    	}
    	String zkPath = "/" + escn.getNodeName();
    	JSONObject jsonMessage = new JSONObject();
        jsonMessage.put("NodeName", escn.getNodeName());
        jsonMessage.put("NodeHost", escn.getNodeHost());
        jsonMessage.put("NodePort", escn.getNodePort());
        jsonMessage.put("CacheStrategy", ((ECSNode)escn).cacheStrategy);
        jsonMessage.put("CacheSize", ((ECSNode)escn).cacheSize);
        jsonMessage.put("State", ((ECSNode)escn).state);
        jsonMessage.put("NodeHash", nodeHash);
        jsonMessage.put("LeftHash", ((ECSNode)escn).leftHash);
        jsonMessage.put("RightHash", ((ECSNode)escn).rightHash);
        jsonMessage.put("Target", ((ECSNode)escn).target);
        jsonMessage.put("Transfer", ((ECSNode)escn).transfer);
        byte[] zkData = jsonMessage.toString().getBytes();
        try {
			this.zk.setData(zkPath, zkData, this.zk.exists(zkPath,true).getVersion());
		} catch (KeeperException e) {
			e.printStackTrace();
			return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
        
    	return true;
    }

    public boolean updateZnodeNodeTarget(IECSNode escn, String target) {
        // update znode
//    	if(((ECSNode)escn).target == target) {
//    		return true;
//    	}
    	String zkPath = "/" + escn.getNodeName();
    	JSONObject jsonMessage = new JSONObject();
        jsonMessage.put("NodeName", escn.getNodeName());
        jsonMessage.put("NodeHost", escn.getNodeHost());
        jsonMessage.put("NodePort", escn.getNodePort());
        jsonMessage.put("CacheStrategy", ((ECSNode)escn).cacheStrategy);
        jsonMessage.put("CacheSize", ((ECSNode)escn).cacheSize);
        jsonMessage.put("State", ((ECSNode)escn).state);
        jsonMessage.put("NodeHash", ((ECSNode)escn).nameHash);
        jsonMessage.put("LeftHash", ((ECSNode)escn).leftHash);
        jsonMessage.put("RightHash", ((ECSNode)escn).rightHash);
        jsonMessage.put("Target", target);
        jsonMessage.put("Transfer", ((ECSNode)escn).transfer);
        byte[] zkData = jsonMessage.toString().getBytes();
        try {
			this.zk.setData(zkPath, zkData, this.zk.exists(zkPath,true).getVersion());
		} catch (KeeperException e) {
			e.printStackTrace();
			return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
        
    	return true;
    }

    public boolean updateZnodeNodeTransfer(IECSNode escn, String transfer) {
        // update znode
//    	if(((ECSNode)escn).target == target) {
//    		return true;
//    	}
    	String zkPath = "/" + escn.getNodeName();
    	JSONObject jsonMessage = new JSONObject();
        jsonMessage.put("NodeName", escn.getNodeName());
        jsonMessage.put("NodeHost", escn.getNodeHost());
        jsonMessage.put("NodePort", escn.getNodePort());
        jsonMessage.put("CacheStrategy", ((ECSNode)escn).cacheStrategy);
        jsonMessage.put("CacheSize", ((ECSNode)escn).cacheSize);
        jsonMessage.put("State", ((ECSNode)escn).state);
        jsonMessage.put("NodeHash", ((ECSNode)escn).nameHash);
        jsonMessage.put("LeftHash", ((ECSNode)escn).leftHash);
        jsonMessage.put("RightHash", ((ECSNode)escn).rightHash);
        jsonMessage.put("Target", ((ECSNode)escn).target);
        jsonMessage.put("Transfer", transfer);
        byte[] zkData = jsonMessage.toString().getBytes();
        try {
			this.zk.setData(zkPath, zkData, this.zk.exists(zkPath,true).getVersion());
		} catch (KeeperException e) {
			e.printStackTrace();
			return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
        
    	return true;
    }

    public boolean addECSNodeToHashRing(IECSNode escn) {
        // Hash the server's name
        String nameHash = hasher.hashString(escn.getNodeName());
        ((ECSNode)escn).nameHash = nameHash;
        HashRingEntry ringEntry = new HashRingEntry(escn, nameHash);

        // Insert the ringEntry into the hash ring that preserves order
        if (this.hashRing.size() == 0) {
            this.hashRing.add(ringEntry);
        } else {
            // Find the index of the position that is less 
            int i = 0;
            while(i < this.hashRing.size()) {
                if (hasher.compareHash(this.hashRing.get(i).hashValue, nameHash) == 1) {
                    // found a bigger hash value
                	break;
                }
                i++;
            }
            this.hashRing.add(i, ringEntry);
            int t = (i + 1)%(this.hashRing.size());
            // Set target
            this.updateZnodeNodeTarget(hashRing.get(t).escn, hashRing.get(i).escn.getNodeName());
            ((ECSNode)hashRing.get(t).escn).target = hashRing.get(i).escn.getNodeName();
            // Set transfer to ON
            this.updateZnodeNodeTransfer(hashRing.get(t).escn, "ON");
            ((ECSNode)hashRing.get(t).escn).transfer = "ON"; 
            this.updateZnodeNodeTransfer(hashRing.get(i).escn, "ON");
            ((ECSNode)hashRing.get(i).escn).transfer = "ON";
        }

        return true;
    }
    
    public boolean removeESCNodeFromHashRing(IECSNode escn) {
    	int i = 0;
    	while(i < this.hashRing.size()) {
    		HashRingEntry r = hashRing.get(i);
    		if (r.escn.getNodeName() == escn.getNodeName()) {
    			// found the ring entry
    			IECSNode e = r.escn;
    			((ECSNode)e).leftHash = "-1";
    			((ECSNode)e).rightHash = "-1";
    			this.updateZnodeHash(e, "-1", "-1");
    			this.hashRing.remove(i);
    			return true;
    		}
    		i++;
    	}
    	return true;
    }

    public boolean recalculateHashRange() {
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
            escn.leftHash = "0"; // Minimal Hash
            escn.rightHash = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"; // Biggest Hash
            status = this.updateZnodeHash(escn, escn.leftHash, escn.rightHash);
            status = this.updateZnodeNodeHash(escn, escn.nameHash);
        } else {
            while(i < numRingEntry) {
                ringEntry = this.hashRing.get(i);
                escn = (ECSNode)ringEntry.escn;
                if(i == 0) {
                	escn.leftHash = hashRing.get(numRingEntry - 1).hashValue;
                	escn.rightHash = ringEntry.hashValue;
                } else {
                    // Last entry
                	escn.leftHash = hashRing.get(i - 1).hashValue;
                    escn.rightHash = hashRing.get(i).hashValue;
                }
                status = this.updateZnodeHash(escn, escn.leftHash, escn.rightHash);
                status = this.updateZnodeNodeHash(escn, escn.nameHash);
                i++;
            }
        }
        return status;
    }


    public boolean startServer(IECSNode escn) {
    	((ECSNode)escn).state = "START";
    	return this.updateZnodeState(escn, "START");
    }


    public boolean stopServer(IECSNode escn) {
    	((ECSNode)escn).state = "STOP";
    	return this.updateZnodeState(escn, "STOP");

    }


    public boolean shutdownServer(IECSNode escn) {
        // Remove the node from hash ring
    	this.removeNode(escn);
    	// remove the znode
    	this.removeZnode(escn);
    	return true;
    }
}