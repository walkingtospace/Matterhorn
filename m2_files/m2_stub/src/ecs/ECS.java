
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
import java.security.Timestamp;

// Internal Import
import common.helper.MD5Hasher;
import common.message.MetaDataEntry;

// JSON
import org.json.simple.JSONArray;
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
        
        this.sshStartFD();

        this.createFDNode();
        
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
//        status = this.sshStartServer(availServer);
        
        // Wait until it is added
        try {
        	System.out.println("start server: " + status);
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        
        // Add the ECSNode to the hashring
        int rIndex = this.addECSNodeToHashRing(availServer);

        if (rIndex != -1) {
        	status = this.recalculateHashRangeSetTransfer(rIndex);
        } else {
        // Configure the hash range of the node
        	status = this.recalculateHashRange();
        }

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
 
    public boolean waitGeneric(int numSec) {
    	try {
    		System.out.println("Waiting transfer to be done");
			Thread.sleep(numSec * 1000);
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
    	this.waitTransfer();
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
		// Check which node has target
		String path = event.getPath();
		path = path.substring(1,path.length());
		JSONObject jsonMessage;
		if (path == "fd") {
			jsonMessage = this.getJSON(path);
	    	JSONArray failedServers = (JSONArray) jsonMessage.get("failed");
	    	// Empty the fd node
	    	JSONArray empty = new JSONArray();
	    	jsonMessage.put("failed", empty);
	        byte[] zkData = jsonMessage.toString().getBytes();
	        try {
				this.zk.setData("/fd", zkData, -1);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	        
	    	// process the failed nodes
	    	String failedServerName;
	    	for (int i = 0; i < failedServers.size(); i++) {
	    		failedServerName = (String)failedServers.get(i);
	    		System.out.println("Handling failed server: " + failedServerName);
	    		// Restart the node via SSH
	    		IECSNode failedServerNode = this.getNodeByKey(failedServerName);
	    		this.sshStartServer(failedServerNode);
	    	}
		} else {
			jsonMessage = this.getJSON(path);
	        String targetname = (String)jsonMessage.get("Target");
	        String transfer = (String)jsonMessage.get("Transfer");
	        if (!targetname.equals("null") && transfer.equals("OFF")) {
	        	System.out.println("print incoing message");
	        	System.out.println(jsonMessage.toString());
	        	JSONObject jsonMessageTarget = this.getJSON(targetname);
	        	String transferTarget = (String)jsonMessageTarget.get("Transfer");
	        	if (transferTarget.equals("ON")) {
	        		System.out.println("pass condiction");
	        		IECSNode sender = this.getNodeByKey(path);
	        		IECSNode receiver = this.getNodeByKey(targetname);
	        		System.out.println("set stuff");
	        		((ECSNode)sender).target = "null";
	        		((ECSNode)sender).transfer = "OFF";
	        		System.out.println("HERHEHREHRHER");
	        		System.out.println(((ECSNode)this.getNodeByKey(path)).transfer);
	        		this.updateZnodeNodeTarget(sender, "null");
	        		((ECSNode)receiver).transfer = "OFF";
	        		this.updateZnodeNodeTransfer(receiver, "OFF");
	        	}
	        }	
		}
    }
    
    public JSONObject getJSON(String nodename) {
		byte[] raw_data = null;
		try {
			raw_data = this.zk.getData("/" + nodename, this, null);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		String jsonstr = new String(raw_data);
		JSONObject jsonMessage = null;
        JSONParser parser = new JSONParser();
        try {
            jsonMessage = (JSONObject) parser.parse(jsonstr);
            return jsonMessage;
        } catch (ParseException e) {
        	e.printStackTrace();
        	return null;
        }
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
                                         "OFF",
                                         "null",
                                         "null",
                                         "null",
                                         "null");
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

    public boolean sshStartServer(IECSNode res) {
        System.out.println("Running SSH to start" + res.getNodeName());
        Process proc;
        //String command = "ssh -n <username>@localhost nohup java -jar java -jar m2-server.jar 0.0.0.0 3200 &";
        String command = "ssh 0.0.0.0 java -jar ~/ECE419/Matterhorn/m2_files/m2_stub/m2-server.jar 0.0.0.0";
        command = command + " " + Integer.toString(this.zkPort);
        command = command + " " + res.getNodeName();
        Runtime run = Runtime.getRuntime();
        System.out.println(command);
        try {
          proc = run.exec(command);
          return true;
        } catch (IOException e) {
          e.printStackTrace();
          return false;
        }	
    }

    public boolean sshStartFD() {
        System.out.println("Running SSH to start" + " Failure Detector");
        Process proc;
        //String command = "ssh -n <username>@localhost nohup java -jar java -jar m2-server.jar 0.0.0.0 3200 &";
        String command = "ssh 0.0.0.0 java -jar ~/ECE419/Matterhorn/m2_files/m2_stub/fd.jar 5 0.0.0.0";
        command = command + " " + Integer.toString(this.zkPort);
        Runtime run = Runtime.getRuntime();
        System.out.println(command);
        try {
          proc = run.exec(command);
          return true;
        } catch (IOException e) {
          e.printStackTrace();
          return false;
        }	
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

    public boolean createFDNode() {
    	String zkPath = "/" +"fd";
    	JSONObject jsonMessage = new JSONObject();
    	JSONArray failedServers = new JSONArray();
    	jsonMessage.put("failed", failedServers);
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
        jsonMessage.put("M1", ((ECSNode)escn).M1);
        jsonMessage.put("M2", ((ECSNode)escn).M2);
        jsonMessage.put("R1", ((ECSNode)escn).R1);
        jsonMessage.put("R2", ((ECSNode)escn).R2);
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
    	JSONObject oldConfig = this.getJSON(escn.getNodeName());
    	oldConfig.put("State", state);
        byte[] zkData = oldConfig.toString().getBytes();
        String zkPath = "/" + escn.getNodeName();
        try {
			this.zk.setData(zkPath, zkData, -1);
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
    	JSONObject oldConfig = this.getJSON(escn.getNodeName());
    	oldConfig.put("LeftHash", leftHash);
    	oldConfig.put("RightHash", rightHash);
        byte[] zkData = oldConfig.toString().getBytes();
        String zkPath = "/" + escn.getNodeName();
        try {
			this.zk.setData(zkPath, zkData, -1);
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
    	JSONObject oldConfig = this.getJSON(escn.getNodeName());
    	oldConfig.put("NodeHash", nodeHash);
        byte[] zkData = oldConfig.toString().getBytes();
        String zkPath = "/" + escn.getNodeName();
        try {
			this.zk.setData(zkPath, zkData, -1);
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
    	JSONObject oldConfig = this.getJSON(escn.getNodeName());
    	oldConfig.put("Target", target);
        byte[] zkData = oldConfig.toString().getBytes();
        String zkPath = "/" + escn.getNodeName();
        try {
			this.zk.setData(zkPath, zkData, -1);
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
    	
    	JSONObject oldConfig = this.getJSON(escn.getNodeName());
    	oldConfig.put("Transfer", transfer);
        byte[] zkData = oldConfig.toString().getBytes();
        String zkPath = "/" + escn.getNodeName();
        try {
			this.zk.setData(zkPath, zkData, -1);
		} catch (KeeperException e) {
			e.printStackTrace();
			return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
        
    	return true;
    }
    
    public boolean updateZnodeLeftRightHashTargetNodeTransfer(IECSNode escn, String leftHash,
    		                                                  String rightHash, String target, String transfer) {
    	JSONObject oldConfig = this.getJSON(escn.getNodeName());
    	oldConfig.put("Transfer", transfer);
    	oldConfig.put("LeftHash", leftHash);
    	oldConfig.put("RightHash", rightHash);
    	oldConfig.put("Target", target);
        byte[] zkData = oldConfig.toString().getBytes();
        String zkPath = "/" + escn.getNodeName();
        try {
			this.zk.setData(zkPath, zkData, -1);
		} catch (KeeperException e) {
			e.printStackTrace();
			return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
    	return true;
    }
    
    public boolean updateZnodeM1M2R1R2(IECSNode escn, String M1, String M2, String R1, String R2) {
    	JSONObject oldConfig = this.getJSON(escn.getNodeName());
    	oldConfig.put("M1", M1);
    	oldConfig.put("M2", M2);
    	oldConfig.put("R1", R1);
    	oldConfig.put("R2", R2);
        byte[] zkData = oldConfig.toString().getBytes();
        String zkPath = "/" + escn.getNodeName();
        try {
			this.zk.setData(zkPath, zkData, -1);
		} catch (KeeperException e) {
			e.printStackTrace();
			return false;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
    	return true;
    }

    public int addECSNodeToHashRing(IECSNode escn) {
        // Hash the server's name
        String nameHash = hasher.hashString(escn.getNodeName());
        ((ECSNode)escn).nameHash = nameHash;
        HashRingEntry ringEntry = new HashRingEntry(escn, nameHash);

        // Insert the ringEntry into the hash ring that preserves order
        if (this.hashRing.size() == 0) {
            this.hashRing.add(ringEntry);
            return -1;
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
            return i;
        }
    }
    
    public boolean removeESCNodeFromHashRing(IECSNode escn) {
    	int i = 0;
    	while(i < this.hashRing.size()) {
    		HashRingEntry r = hashRing.get(i);
    		if (r.escn.getNodeName() == escn.getNodeName()) {
    			// found the ring entry
    			IECSNode e = r.escn;
    			int t = (i + 1) % (this.hashRing.size()); // next node
    			IECSNode te = this.hashRing.get(t).escn;
    			((ECSNode)e).leftHash = "-1";
    			((ECSNode)e).rightHash = "-1";
    			((ECSNode)e).transfer = "ON";
    			((ECSNode)te).transfer = "ON";
    			((ECSNode)e).target = te.getNodeName();
    			
    			// Update e node on Zookeeper in a batch
    			this.updateZnodeLeftRightHashTargetNodeTransfer(e, "-1", "-1", te.getNodeName(), "ON");
    			this.updateZnodeNodeTransfer(te, "ON");

    			// remove from hash ring
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
            escn.M1 = "null";
            escn.M2 = "null";
            escn.R1 = "null";
            escn.R2 = "null";
            this.updateZnodeM1M2R1R2(escn, "null", "null", "null", "null");
        } else {
            while(i < numRingEntry) {
                ringEntry = this.hashRing.get(i);
                escn = (ECSNode)ringEntry.escn;
                
                // Set M1, M2 and R1 and R2 for that particular node
                int m1i = (i - 1) % numRingEntry;
                if (m1i < 0 ) {
                	m1i = numRingEntry + m1i;
                }
                String m1name = (m1i == i) ? "null" : this.hashRing.get(m1i).escn.getNodeName() ;
                
                int m2i = (i - 2) % numRingEntry;
                if (m2i < 0 ) {
                	m2i = numRingEntry + m2i;
                }
                String m2name = (m2i == i) ? "null" : this.hashRing.get(m2i).escn.getNodeName() ;
                
                int r1i = (i + 1) % numRingEntry;
                if (r1i < 0 ) {
                	r1i = numRingEntry + r1i;
                }
                String r1name = (r1i == i) ? "null" : this.hashRing.get(r1i).escn.getNodeName() ;
                
                int r2i = (i + 2) % numRingEntry;
                if (r2i < 0 ) {
                	r2i = numRingEntry + r2i;
                }
                String r2name = (r2i == i) ? "null" : this.hashRing.get(r2i).escn.getNodeName() ;
                
                escn.M1 = m1name;
                escn.M2 = m2name;
                escn.R1 = r1name;
                escn.R2 = r2name;
                this.updateZnodeM1M2R1R2(escn, m1name, m2name, r1name, r2name);
                
                // Update Hash Range
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

    public boolean recalculateHashRangeSetTransfer(int rindex) {
        // find all escn entry in the Hash Ring
        int numRingEntry = this.hashRing.size();
        int i = 0;
        int sindex = (rindex + 1) % numRingEntry;
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
            escn.M1 = "null";
            escn.M2 = "null";
            escn.R1 = "null";
            escn.R2 = "null";
            this.updateZnodeM1M2R1R2(escn, "null", "null", "null", "null");
        } else {
            while(i < numRingEntry) {
                ringEntry = this.hashRing.get(i);
                escn = (ECSNode)ringEntry.escn;
                
                // Set M1, M2 and R1 and R2 for that particular node
                int m1i = (i - 1) % numRingEntry;
                if (m1i < 0 ) {
                	m1i = numRingEntry + m1i;
                }
                String m1name = (m1i == i) ? "null" : this.hashRing.get(m1i).escn.getNodeName() ;
                
                int m2i = (i - 2) % numRingEntry;
                if (m2i < 0 ) {
                	m2i = numRingEntry + m2i;
                }
                String m2name = (m2i == i) ? "null" : this.hashRing.get(m2i).escn.getNodeName() ;
                
                int r1i = (i + 1) % numRingEntry;
                if (r1i < 0 ) {
                	r1i = numRingEntry + r1i;
                }
                String r1name = (r1i == i) ? "null" : this.hashRing.get(r1i).escn.getNodeName() ;
                
                int r2i = (i + 2) % numRingEntry;
                if (r2i < 0 ) {
                	r2i = numRingEntry + r2i;
                }
                String r2name = (r2i == i) ? "null" : this.hashRing.get(r2i).escn.getNodeName() ;
                
                escn.M1 = m1name;
                escn.M2 = m2name;
                escn.R1 = r1name;
                escn.R2 = r2name;
                this.updateZnodeM1M2R1R2(escn, m1name, m2name, r1name, r2name);
                
                // Set Hash Range and Transfer
                if(i == 0) {
                	escn.leftHash = hashRing.get(numRingEntry - 1).hashValue;
                	escn.rightHash = ringEntry.hashValue;
                } else {
                    // Last entry
                	escn.leftHash = hashRing.get(i - 1).hashValue;
                    escn.rightHash = hashRing.get(i).hashValue;
                }
                if (i == rindex) {
                	// receiver
                	status = this.updateZnodeLeftRightHashTargetNodeTransfer(escn, escn.leftHash, escn.rightHash, escn.target, "ON");
                	escn.transfer = "ON";
                    status = this.updateZnodeNodeHash(escn, escn.nameHash);
                } else if (i == sindex) {
                	// sender
                	escn.target = hashRing.get(rindex).escn.getNodeName();
                	status = this.updateZnodeLeftRightHashTargetNodeTransfer(escn, escn.leftHash, escn.rightHash, hashRing.get(rindex).escn.getNodeName(), "ON");
                	escn.transfer = "ON";
                    status = this.updateZnodeNodeHash(escn, escn.nameHash);	

                } else {
                    status = this.updateZnodeHash(escn, escn.leftHash, escn.rightHash);
                    status = this.updateZnodeNodeHash(escn, escn.nameHash);	
                }
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
