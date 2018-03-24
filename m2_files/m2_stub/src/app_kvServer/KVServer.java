package app_kvServer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;


import org.apache.log4j.Logger;
//import org.apache.log4j.Level;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import cache.KVCache;
import cache.KVFIFOCache;
import cache.KVLFUCache;
import cache.KVLRUCache;
import client.KVStore;
import common.helper.MD5Hasher;
import common.message.MetaDataEntry;

public class KVServer implements IKVServer, Watcher {

    /**
     * Start KV Server with selected name
     * @param name          unique name of server
     * @param zkHostname    hostname where zookeeper is running
     * @param zkPort        port where zookeeper is running
     */

    private static Logger logger = Logger.getRootLogger();
    private boolean running;
    private ServerSocket serverSocket;
    
    private int cacheSize;
    private CacheStrategy strategy;
    private KVCache cache;
    
    private String dbPath = "./db/";
    private String replicaPath;
    private String zkPath = "/";
    private String zkHostname;
    private int zkPort;
    
    private String master1Name;
    private String master2Name;
    
    private String replica1Name;
    private String replica2Name;
    
    private KVStore replica1Client;
    private KVStore replica2Client;
    
    private MetaDataEntry metaDataEntry;
    private String name;
    private volatile boolean writeLock;
    
    private volatile String state;
    
    private ZooKeeper zk;
    private MD5Hasher hasher;

    public KVServer(String name,
                    String zkHostname,
                    int zkPort) {
    	
    	this.replica1Client = null;
    	this.replica2Client = null;
    	
    	this.name = name;
        
        this.zkHostname = zkHostname;
        this.zkPort = zkPort;
        File dbDir = new File(dbPath);
        try{
            dbDir.mkdir();
        }
        catch(SecurityException se){
//            logger.error("Error! Can't create database folder");
        	System.out.println("Error! Can't create database folder");
        }
        this.dbPath += name + "/";
        this.replicaPath = this.dbPath + "replica/";
        try {
			hasher = new MD5Hasher();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
        
        String connection = zkHostname + ":" + Integer.toString(zkPort) + zkPath + name;
        try {
			this.zk = new ZooKeeper(connection, 3000, this);
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        
        JSONObject jsonMessage = null;
		try {
			jsonMessage = this.retrieveZnodeData("");
		} catch (IOException | KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
        
        this.state = (String) jsonMessage.get("State");
        if (jsonMessage.get("Transfer").toString().equals("ON")) {
        	this.lockWrite();
        } else {
        	this.unlockWrite();
        }
        int cacheSize = Integer.parseInt(jsonMessage.get("CacheSize").toString());
        
        String strategy = (String) jsonMessage.get("CacheStrategy");

        MetaDataEntry metaDataEntry = this.fillUpMetaDataEntry(jsonMessage);
        
        try {
			this.initKVServer(metaDataEntry, cacheSize, strategy);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        
        this.assignMasterAndReplica(jsonMessage);
    }
    
    private MetaDataEntry fillUpMetaDataEntry(JSONObject jsonMessage) {
    	
    	String serverHost = (String)jsonMessage.get("NodeHost");
        int serverPort = Integer.parseInt(jsonMessage.get("NodePort").toString());
        String leftHash = (String)jsonMessage.get("LeftHash");
        String rightHash = (String)jsonMessage.get("RightHash");
        MetaDataEntry metaDataEntry = new MetaDataEntry(name, serverHost, serverPort, leftHash, rightHash);
        return metaDataEntry;
    }
    
    private void assignMasterAndReplica(JSONObject jsonMessage) {
    	this.master1Name =  (String) jsonMessage.get("M1");
    	this.master2Name =  (String) jsonMessage.get("M2");
        
    	this.replica1Name =  (String) jsonMessage.get("R1");
    	this.replica2Name =  (String) jsonMessage.get("R2");
    	if (!this.replica1Name.equals("null")) {
    		this.replica1Client = replicaData(this.replica1Name);
    	}
    	if (!this.replica2Name.equals("null")) {
    		this.replica2Client = replicaData(this.replica2Name);
    	}
    }
    
    private JSONObject decodeJsonStr(String data) {
    	JSONObject jsonMessage = null;
        JSONParser parser = new JSONParser();
        try {
            jsonMessage = (JSONObject) parser.parse(data);
        } catch (ParseException e) {
            logger.error("Error! " +
                    "Unable to parse incoming bytes to json. \n", e);
        }
        return jsonMessage;
    }
    
    
    public void initKVServer(MetaDataEntry metaDataEntry, int cacheSize, String strategy) throws KeeperException, InterruptedException {
    	
    	this.metaDataEntry = metaDataEntry;
    	this.cacheSize = cacheSize;
        this.strategy = CacheStrategy.valueOf(strategy);
        this.cache = createCache(this.strategy);
        	
        File dbDir = new File(this.dbPath);
        File replicaDir = new File (this.replicaPath);
        try{
            dbDir.mkdir();
            replicaDir.mkdir();
        }
        catch(SecurityException se){
//            logger.error("Error! Can't create database folder");
        	System.out.println("Error! Can't create database folder");
        }
//        this.run();
    }
    
    public List<MetaDataEntry> fillMetaData() throws IOException, KeeperException, InterruptedException {
    	List<MetaDataEntry> metaData = new ArrayList<>();
    	String connection = this.zkHostname + ":" + Integer.toString(this.zkPort) + zkPath;
    	ZooKeeper rootZk = new ZooKeeper(connection, 3000, null);
    	List<String> zNodes = rootZk.getChildren(zkPath, false);
    	for (String zNode: zNodes) {
    		if (!zNode.equals("zookeeper") && !zNode.equals("fd")) {
    			System.out.println("znode: " + zNode);
        		byte[] raw_data = rootZk.getData(zkPath + zNode, false, null);
                String data = new String(raw_data);
                
                JSONObject jsonMessage = decodeJsonStr(data);
                System.out.println("in fillMetaData");
                System.out.println(jsonMessage.toString());
                
                
                MetaDataEntry nodeMetaDataEntry = this.fillUpMetaDataEntry(jsonMessage);
                if (!nodeMetaDataEntry.leftHash.equals("-1"))
                	metaData.add(nodeMetaDataEntry);
    		}
    	}
    	return metaData;
    }
    
    // invoker should provide zkHostname, zkPort, name and zkPath
    public static void main(String[] args) {
        KVServer server = null;
		try {
			
			if (args.length != 3) {
				System.out.println("Wrong number of arguments passed to server");
			}	        
//			new LogSetup("logs/server.log", Level.ALL);
			String zkHostname = args[0];
			int zkPort = Integer.parseInt(args[1]);
			String name = args[2];
			server = new KVServer(name, zkHostname, zkPort);
			server.run();
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
            server.close();
        }
	}



    @Override
    public int getPort(){
        return this.metaDataEntry.serverPort;
    }


    @Override
    public String getHostname(){
        return this.metaDataEntry.serverHost;
    }


    @Override
    public CacheStrategy getCacheStrategy(){
        return this.strategy;
    }


    @Override
    public int getCacheSize(){
        return this.cacheSize;
    }

    public String getState() {
    	return this.state;
    }
    
    @Override
    public boolean inStorage(String key){
        boolean result = false;
        key += ".kv";
        File kvFile = new File(dbPath + key);
        if (kvFile.exists()) {
            result = true;
        }
        return result;
    }


    @Override
    public boolean inCache(String key){
        if (cache != null && cache.get(key) != null) {
            return true;
        }
        return false;
    }


    @Override
    public String getKV(String key) throws Exception{
    	String value = null;
        if (cache != null) {
            value = cache.get(key);
            if (value != null) {
                return value;
            }
        }
        key += ".kv";
        try {
        	value = getValueFromFile(dbPath + key);
            return value;
        }
        catch(FileNotFoundException ex) {
            logger.error("Error! " +
                "Unable to open kv file '" + 
                key + "'" + ex);
        }
        catch(IOException ex) {
            logger.error(
                "Error! reading file '" 
                + key + "'" + ex);
        }
        return value;
    }

    public boolean isKeyInRange(String key, String leftHash, String rightHash) throws NoSuchAlgorithmException {
    	
    	String hashedKey = hasher.hashString(key);
    	System.out.println(key + " and " + hashedKey);
    	if (leftHash == null && rightHash == null) {
    		leftHash = this.metaDataEntry.leftHash;
    		rightHash = this.metaDataEntry.rightHash;
    		System.out.println("retrive from metaData");
    	}
    	System.out.println("left: " + leftHash + " right: " + rightHash);
    	
    	boolean isCrossRange = hasher.compareHash(leftHash, rightHash) == 1;
    	boolean isNormalCase = hasher.compareHash(hashedKey, leftHash) == 1 && hasher.compareHash(hashedKey, rightHash) == -1;
    	boolean isCrossCaseRight = hasher.compareHash(hashedKey, leftHash) == -1 && hasher.compareHash(hashedKey, rightHash) == -1;
    	boolean isCrossCaseLeft = hasher.compareHash(hashedKey, leftHash) == 1 && hasher.compareHash(hashedKey, rightHash) == 1;
    	boolean isBoundaryCase = hasher.compareHash(hashedKey, rightHash) == 0;
    	if (isNormalCase || isBoundaryCase || (isCrossRange && (isCrossCaseRight || isCrossCaseLeft))) {
    		return true;
    	}
    	return false;
    }

    @Override
    public void putKV(String key, String value) throws Exception{
        if (cache != null) {
            if (value == cache.get(key)) {
                // Update recency in case of LRU or count in case of LFU.
                this.cache.set(key, value);
                // Avoid writing to DB.
                return;
            }
            this.cache.set(key, value);
        }
        String fileName = key +".kv";
        File kvFile = new File(dbPath + fileName);
        if (!kvFile.exists()) {
            try {
                kvFile.createNewFile();
            } catch (IOException e1) {
                logger.error("Error! " +
                        "Unable to create key-value file '" + 
                        key + "'");  
            }
        } 
        try {
            FileWriter fileWriter = new FileWriter(kvFile);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(value);
            bufferedWriter.close();
        }
        catch(FileNotFoundException ex) {
            logger.error("Error! " +
                "Unable to open kv file '" + 
                key + "'" + ex);
        }
        catch(IOException ex) {
            logger.error(
                "Error! reading file '" 
                + key + "'");
        }
        this.remoteReplicate(key, value);
    }
    
    @Override
	public boolean deleteKV(String key) throws Exception{
    	boolean result = false;
    	if (inCache(key))
    		this.cache.delete(key);
    	String fileName = key +".kv";
    	File kvFile = new File(this.dbPath + fileName);
    	System.out.println("delete file in path: " + kvFile);
    	if (kvFile.exists()) {
    		kvFile.delete();
    		result = true;
    	}
    	this.remoteReplicate(key, "");
    	return result;
	}
    
    public boolean deleteKVReplica(String key) throws Exception {
    	String fileName = key +".kv";
    	File kvFile = new File(this.replicaPath + fileName);
    	System.out.println("delete file in path: " + kvFile);
    	if (kvFile.exists()) {
    		kvFile.delete();
    	}
    	return true;
    }
    
    public void putKVReplica(String key, String value) throws Exception {
    	String fileName = key +".kv";
        File kvFile = new File(this.replicaPath + fileName);
        if (!kvFile.exists()) {
            try {
                kvFile.createNewFile();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        } 
        try {
            FileWriter fileWriter = new FileWriter(kvFile);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(value);
            bufferedWriter.close();
        }
        catch(FileNotFoundException ex) {
            logger.error("Error! " +
                "Unable to open kv file '" + 
                key + "'" + ex);
        }
        catch(IOException ex) {
            logger.error(
                "Error! reading file '" 
                + key + "'");
        }
    }
    
    
    
    private void remoteReplicate(String key, String value) {
    	System.out.println("in remoteReplicate function");
    	try {
	    	if (this.replica1Client != null) {
	    		System.out.println("replica to: " + this.replica1Name);
	    		this.replica1Client.put(key, value);
	    	}
	    	if (this.replica2Client != null) {
	    		System.out.println("replica to: " + this.replica2Name);
	    		this.replica2Client.put(key, value);
	    	}
    	} catch(Exception e) {
    		e.printStackTrace();
    	}
    }


    @Override
    public void clearCache(){
        this.cache = createCache(strategy);
    }


    @Override
    public void clearStorage(){
        File[] files = new File(dbPath).listFiles();
        for (File file: files) {
            if (file.toString().endsWith(".kv")) {
                file.delete();
            }
        }
        
    }


    @Override
    public void run(){
        running = initializeServer();
        if(serverSocket != null) {
            while(running){
                try {
                    Socket client = serverSocket.accept();                
                    ClientConnection connection = new ClientConnection(client, this);
                    new Thread(connection).start();
                    
//                    logger.info("Connected to " 
//                            + client.getInetAddress().getHostName() 
//                            +  " on port " + client.getPort());
                    System.out.println("Connected to " 
                            + client.getInetAddress().getHostName() 
                            +  " on port " + client.getPort());
                } catch (IOException e) {
//                    logger.error("Error! " +
//                            "Unable to establish connection. \n", e);
                	System.out.println("Error! " +
                            "Unable to establish connection. \n" + e);
                }
            }
        }
//        logger.info("Server stopped.");
        System.out.println("Server stopped.");
    }


    @Override
    public void kill(){
        this.stopServer();
    }


    @Override
    public void close(){
        this.stopServer();
    }


    @Override
    public void start() {
        this.state = "START";
    }


    @Override
    public void stop() {
    	this.state = "STOP";
    }


    @Override
    public void lockWrite() {
    	this.writeLock = true;
    }


    @Override
    public void unlockWrite() {
    	this.writeLock = false;
    }
    
    public boolean isLocked() {
    	return this.writeLock;
    }
    
    @Override
    public void update(MetaDataEntry metaDataEntry) {
    	this.metaDataEntry = metaDataEntry;
    }

    private JSONObject retrieveZnodeData(String nodeName) throws IOException, KeeperException, InterruptedException {
    	byte[] raw_data;
    	if (nodeName.equals("")) {
    		raw_data = zk.getData(zkPath, this, null);
    	} else {
    		String connection = this.zkHostname + ":" + Integer.toString(this.zkPort) + zkPath;
    		ZooKeeper rootZk = new ZooKeeper(connection, 3000, null);
    		raw_data = rootZk.getData(zkPath + nodeName, false, null);
    		rootZk.close();
    	}
    	String data = new String(raw_data);
    	
    	JSONObject jsonMessage = this.decodeJsonStr(data);
        return jsonMessage;
    }

    @Override
    public boolean moveData(String[] hashRange, String targetName){
    	
    	JSONObject jsonMessage;
		try {
			jsonMessage = this.retrieveZnodeData(targetName);
			
			String targetServerHost = (String) jsonMessage.get("NodeHost");
	        int targetServerPort = Integer.parseInt(jsonMessage.get("NodePort").toString());
	        System.out.println(targetServerHost + " " + targetServerPort);
	    	KVStore migrationClient = new KVStore(targetServerHost, targetServerPort);
	    	migrationClient.connect();
	    	System.out.println("pass migrationClient connect");
	    	migrationClient.enableTransfer();
	    	File[] files = new File(dbPath).listFiles();
	    	System.out.println("size of db: " + files.length);
            System.out.println("hash range in moveData: " + hashRange);
	        for (File file: files) {
	        	String fileName = file.getName();
	            if (fileName.endsWith(".kv")) {
	            	System.out.println("file in moveData: " + fileName);
	                String key = fileName.substring(0, fileName.length() - 3);
	                String hashedKey = hasher.hashString(key);
	                System.out.println("key in moveData: " + key);
	                if (this.isKeyInRange(key, hashRange[0], hashRange[1])) {
	                	String value = getValueFromFile(dbPath + fileName);
	                	System.out.println("send key: " + key);
	                	migrationClient.put(key, value);
	                }
	            }
	        }
	        migrationClient.disconnect();
	        System.out.println("after disconnect in moveData");
		} catch (Exception e) {
			System.out.println("moveData exception");
			e.printStackTrace();
			return false;
		}
		System.out.println("finish moveData");
        return true;
    }
    
    private KVStore replicaData(String replicaName) {
    	JSONObject jsonMessage;
    	KVStore migrationClient;
		try {
			jsonMessage = this.retrieveZnodeData(replicaName);
			
			String replicaServerHost = (String) jsonMessage.get("NodeHost");
	        int replicaServerPort = Integer.parseInt(jsonMessage.get("NodePort").toString());

	    	migrationClient = new KVStore(replicaServerHost, replicaServerPort);
	    	migrationClient.connect();
	    	migrationClient.enableReplicate();
	    	File[] files = new File(dbPath).listFiles();
	        for (File file: files) {
	        	String fileName = file.getName();
	            if (fileName.endsWith(".kv")) {
	                String key = fileName.substring(0, fileName.length() - 3);
	                String hashedKey = hasher.hashString(key);
	                if (this.isKeyInRange(key, this.metaDataEntry.leftHash, this.metaDataEntry.rightHash)) {
	                	String value = getValueFromFile(dbPath + fileName);
	                	migrationClient.put(key, value);
	                }
	            }
	        }
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		System.out.println("finish replicaData");
        return migrationClient;
    }
    
    private void notifyECS(JSONObject jsonMessage) {
        byte[] zkData = jsonMessage.toString().getBytes();
        try {
			this.zk.setData(zkPath, zkData, -1);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
    }
    
    private String getValueFromFile(String path) throws IOException {
    	File kvFile = new File(path);
    	String value = null;
        if (kvFile.exists()) {
            FileReader fileReader = new FileReader(kvFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            value = bufferedReader.readLine();
            bufferedReader.close();
        }
        return value;
    }
    
    private void stopServer() {
    	if (this.replica1Client != null) {
    		this.replica1Client.disconnect();
    		this.replica1Client = null;
    	}
    	if (this.replica2Client != null) {
    		this.replica2Client.disconnect();
    		this.replica2Client = null;
    	}
        running = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            // logger.error("Error! " +
            //         "Unable to close socket on port: " + getPort(), e);
        	System.out.println("Error! " + "Unable to close socket on port: " + getPort());
        }
    }


    private KVCache createCache(CacheStrategy strategy){
        KVCache cache = null;
        switch (strategy) {
            case LRU:
                cache = new KVLRUCache(this.cacheSize);
                break;
            case FIFO:
                cache = new KVFIFOCache(this.cacheSize);
                break;
            case LFU:
                cache = new KVLFUCache(this.cacheSize);
            default:
                break;
        }
        return cache;
    }


    private boolean initializeServer() {
//        logger.info("Initialize server ...");
    	System.out.println("Initialize server ...");
        try {
            serverSocket = new ServerSocket(getPort());
            System.out.println("Server listening on port: " 
                    + serverSocket.getLocalPort());    
//            logger.info("Server listening on port: " 
//                    + serverSocket.getLocalPort());    
            return true;
        
        } catch (IOException e) {
//            logger.error("Error! Cannot open server socket:");
        	System.out.println("Error! Cannot open server socket:");
            if(e instanceof BindException){
//                logger.error("Port " + getPort() + " is already bound!");
            	System.out.println("Port " + getPort() + " is already bound!");
            }
            return false;
        }
    }
    
    private boolean deleteInRangeKeyReplica(String leftHash, String rightHash) {
    	File[] files = new File(dbPath).listFiles();
    	boolean result = true;
        for (File file: files) {
        	String fileName = file.getName();
            if (fileName.endsWith(".kv")) {
                String key = fileName.substring(0, fileName.length() - 3);
                System.out.println(key + " is deleted in master");
            	try {
					if (this.isKeyInRange(key, leftHash, rightHash)) {
						if (inCache(key))
				    		cache.delete(key);
				    	File kvFile = new File(dbPath + fileName);
				    	if (kvFile.exists()) {
				    		kvFile.delete();
				    	}
			    		this.remoteReplicate(key, "");
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
                
            }
        }
        return result;
    }
    
    private boolean deleteInRangeKeyLocal(String leftHash, String rightHash) {
    	File[] files = new File(dbPath).listFiles();
    	boolean result = true;
        for (File file: files) {
        	String fileName = file.getName();
            if (fileName.endsWith(".kv")) {
                String key = fileName.substring(0, fileName.length() - 3);
                System.out.println(key + " is deleted");
            	try {
					if (this.isKeyInRange(key, leftHash, rightHash)) {
						if (inCache(key))
				    		cache.delete(key);
				    	File kvFile = new File(dbPath + fileName);
				    	if (kvFile.exists()) {
				    		kvFile.delete();
				    	}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
                
            }
        }
        return result;
    }
    
    private boolean deleteInRangeKeyRemote(String leftHash, String rightHash) {
    	File[] files = new File(dbPath).listFiles();
    	boolean result = true;
        for (File file: files) {
        	String fileName = file.getName();
            if (fileName.endsWith(".kv")) {
                String key = fileName.substring(0, fileName.length() - 3);
                System.out.println(key + " is deleted");
            	try {
					if (this.isKeyInRange(key, leftHash, rightHash)) {
						this.remoteReplicate(key, "");
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
                
            }
        }
        return result;
    }
    
    private void clearReplica() {
    	File[] files = new File(this.replicaPath).listFiles();
    	for (File file: files) {
    		String fileName = file.getName();
            if (fileName.endsWith(".kv")) {
            	File kvFile = new File(this.replicaPath + fileName);
            	kvFile.delete();
            }
    	}
    }
    
    private boolean deleteInRangeKey(String leftHash, String rightHash) {
    	File[] files = new File(dbPath).listFiles();
    	boolean result = true;
        for (File file: files) {
        	String fileName = file.getName();
            if (fileName.endsWith(".kv")) {
                String key = fileName.substring(0, fileName.length() - 3);
                System.out.println(key + " is deleted");
            	try {
					if (this.isKeyInRange(key, leftHash, rightHash)) {
						if (inCache(key))
				    		cache.delete(key);
				    	File kvFile = new File(dbPath + fileName);
				    	kvFile.delete();
			    		this.remoteReplicate(key, "");
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
                
            }
        }
        return result;
    }

	@Override
	public void process(WatchedEvent event) {
		System.out.println("triggered");
		JSONObject jsonMessage;
		try {
			jsonMessage = this.retrieveZnodeData("");
		} catch (IOException | KeeperException | InterruptedException e1) {
			e1.printStackTrace();
			return;
		}
		
        System.out.println(jsonMessage.toString());
		EventType type = event.getType();
		System.out.println(type);
		switch (type) {
			case NodeDataChanged:
				String state = (String) jsonMessage.get("State");
				String targetName = (String) jsonMessage.get("Target");
				String transferState = (String) jsonMessage.get("Transfer");
                String newLeftHash = (String) jsonMessage.get("LeftHash");
                
                String newMaster1Name = (String) jsonMessage.get("M1");
                String newMaster2Name = (String) jsonMessage.get("M2");
                String newReplica1Name = (String) jsonMessage.get("R1");
                String newReplica2Name = (String) jsonMessage.get("R2");
                
                boolean isTransfer = !targetName.equals("null") && transferState.equals("ON");
                boolean isMaster1Changed = !this.master1Name.equals(newMaster1Name);
                boolean isMaster2Changed = !this.master2Name.equals(newMaster2Name);
                boolean isReplica1Changed = !this.replica1Name.equals(newReplica1Name);
                boolean isReplica2Changed = !this.replica2Name.equals(newReplica2Name);
                
                boolean isNodeDeleted = newLeftHash.equals("-1");
				System.out.println(transferState);
				if (state.equals("START")) {
					this.start();
				} else if (state.equals("STOP") || isNodeDeleted) {
					this.stop();
				}
				if (transferState.equals("ON")) {
					this.lockWrite();
				} else {
					this.unlockWrite();
				}
				
				if (isTransfer) {
					String[] hashRange = new String[2];
					
					if (isNodeDeleted) {
						System.out.println("removeNode case");
						hashRange[0] = this.metaDataEntry.leftHash;
						hashRange[1] = this.metaDataEntry.rightHash;
					} else {
						System.out.println("addeNode case");
						if (this.metaDataEntry.leftHash.equals("0") && this.metaDataEntry.rightHash.equals("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")) {
							hashRange[0] = (String) jsonMessage.get("NodeHash");							
						} else {
							hashRange[0] = this.metaDataEntry.leftHash;
						}
						hashRange[1] = newLeftHash;
					}
					System.out.println(hashRange);
					this.deleteInRangeKeyRemote(hashRange[0], hashRange[1]);
					boolean result;
					try {
						result = this.moveData(hashRange, targetName);
						if (!result)
							System.out.println("data transfer failed");
					} catch (Exception e) {
						e.printStackTrace();
					}
					this.deleteInRangeKeyLocal(hashRange[0], hashRange[1]);
					jsonMessage.put("Transfer", "OFF");
                	this.notifyECS(jsonMessage);
                    if (isNodeDeleted) {
                    	this.clearReplica();
                        this.close();
                        System.out.println("server is closed due to node is removed");
                    }
				}
                MetaDataEntry metaDataEntry = this.fillUpMetaDataEntry(jsonMessage);
                this.update(metaDataEntry);
				System.out.println(this.metaDataEntry.leftHash  + " " + this.metaDataEntry.rightHash);
				
				String tempLeftHash, tempRightHash;
				if (isMaster1Changed) {
					if (!this.master1Name.equals("null")) {
						try {
							jsonMessage = this.retrieveZnodeData(this.master1Name);
						} catch (Exception e) {
							e.printStackTrace();
							return;
						}
						tempLeftHash = (String) jsonMessage.get("LeftHash");
						tempRightHash = (String) jsonMessage.get("RightHash");
						if (!tempLeftHash.equals("-1")) {
							this.deleteInRangeKeyReplica(tempLeftHash, tempRightHash);
						}
					}
					this.master1Name = newMaster1Name;
				}
				if (isMaster2Changed) {
					if (!this.master2Name.equals("null")) {
						try {
							jsonMessage = this.retrieveZnodeData(this.master2Name);
						} catch (Exception e) {
							e.printStackTrace();
							return;
						}
						tempLeftHash = (String) jsonMessage.get("LeftHash");
						tempRightHash = (String) jsonMessage.get("RightHash");
						if (!tempLeftHash.equals("-1")) {
							this.deleteInRangeKeyReplica(tempLeftHash, tempRightHash);
						}
					}
					this.master2Name = newMaster2Name;
				}
				if (isReplica1Changed) {
					if (replica1Client != null)
						this.replica1Client.disconnect();
					if (!newReplica1Name.equals("null")) {
						this.replica1Client = this.replicaData(newReplica1Name);
					} else {
						this.replica1Client = null;
					}
					this.replica1Name = newReplica1Name;
				}
				if (isReplica2Changed) {	
					if (replica2Client != null)
						this.replica2Client.disconnect();
					if (!newReplica1Name.equals("null")) {
						this.replica2Client = this.replicaData(newReplica2Name);
					} else {
						this.replica2Client = null;
					}
					this.replica2Name = newReplica2Name;
				}
				break;
			case NodeDeleted:
				this.close();
				break;	
			default:
                break;
		}
		System.out.println("done trigger");
	}
}