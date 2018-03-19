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
    private String zkPath = "/";
    private String zkHostname;
    private int zkPort;
    
    private MetaDataEntry metaDataEntry;
    private String name;
    private volatile boolean writeLock;
    
    private volatile String state;
    
    private ZooKeeper zk;
    private MD5Hasher hasher;

    public KVServer(String name,
                    String zkHostname,
                    int zkPort) {
        /* KenNote: The func signature is different in M2 */
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
        
        String strategy = (String)jsonMessage.get("CacheStrategy");

        MetaDataEntry metaDataEntry = this.fillUpMetaDataEntry(jsonMessage);
        try {
			this.initKVServer(metaDataEntry, cacheSize, strategy);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
    
    private MetaDataEntry fillUpMetaDataEntry(JSONObject jsonMessage) {
    	String serverHost = (String)jsonMessage.get("NodeHost");
        int serverPort = Integer.parseInt(jsonMessage.get("NodePort").toString());
        String leftHash = (String)jsonMessage.get("LeftHash");
        String rightHash = (String)jsonMessage.get("RightHash");

        MetaDataEntry metaDataEntry = new MetaDataEntry(name, serverHost, serverPort, leftHash, rightHash);
        return metaDataEntry;
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
        	
        File dbDir = new File(dbPath);
        try{
            dbDir.mkdir();
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
    		if (!zNode.equals("zookeeper")) {
    			System.out.println("znode: " + zNode);
        		byte[] raw_data = rootZk.getData(zkPath + zNode, false, null);
                String data = new String(raw_data);
                
                JSONObject jsonMessage = decodeJsonStr(data);
                System.out.println("in fillMetaData");
                System.out.println(jsonMessage.toString());
                
                
                MetaDataEntry nodeMetaDataEntry = this.fillUpMetaDataEntry(jsonMessage);
                metaData.add(nodeMetaDataEntry);
    		}
    	}
    	return metaData;
    }
    
    // invoker should provide zkHostname, zkPort, name and zkPath
    public static void main(String[] args) {
		try {
			if (args.length != 3) {
				System.out.println("Wrong number of arguments passed to server");
			}
//			new LogSetup("logs/server.log", Level.ALL);
			String zkHostname = args[0];
			int zkPort = Integer.parseInt(args[1]);
			String name = args[2];
//			String name = "server1";
//			String zkHostname = "0.0.0.0";
//			int zkPort = 3100;
			KVServer server = new KVServer(name, zkHostname, zkPort);
			server.run();
		} catch(Exception e) {
			logger.error("Error! Can't start server");
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
//    	if ((hasher.compareHash(leftHash, hashedKey) == -1 && hasher.compareHash(rightHash, hashedKey) == 1)
//    			|| (hasher.compareHash(hashedKey, rightHash) == 0)
//    			|| (hasher.compareHash(leftHash, rightHash) == 1 && (((hasher.compareHash(leftHash, hashedKey) == 1) && (hasher.compareHash(rightHash, hashedKey) == 1)) 
//    			|| ((hasher.compareHash(leftHash, hashedKey) == -1) && (hasher.compareHash(rightHash, hashedKey) == -1))))) {
//    		return true;
//    	}
    	return false;
    }

    @Override
    public void putKV(String key, String value) throws Exception{
        if (cache != null) {
            if (value == cache.get(key)) {
                // Update recency in case of LRU or count in case of LFU.
                cache.set(key, value);
                // Avoid writing to DB.
                return;
            }
            cache.set(key, value);
        }
        key += ".kv";
        File kvFile = new File(dbPath + key);
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
    }
    
    @Override
	public boolean deleteKV(String key) throws Exception{
//    	if (writeLock == true)
//    		return false;
		boolean result = false;
    	if (inCache(key))
    		cache.delete(key);
    	key += ".kv";
    	File kvFile = new File(dbPath + key);
    	if (kvFile.exists()) {
    		kvFile.delete();
    		result = true;
    	}
    	return result;
	}


    @Override
    public void clearCache(){
        cache = createCache(strategy);
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
//        String time = Long.toString(System.currentTimeMillis());
//        File timeFile = new File(dbPath + time);
//        
//        if (!timeFile.exists()) {
//            try {
//            	timeFile.createNewFile();
//            } catch (IOException e1) {
//                
//            }
//        } 
        
//        System.out.println(System.currentTimeMillis());
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
        stopServer();
    }


    @Override
    public void close(){
        stopServer();
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
    	writeLock = true;
    }


    @Override
    public void unlockWrite() {
    	writeLock = false;
    }
    
    public boolean isLocked() {
    	return writeLock;
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
    // assume hashRange is given in clockwise rotation
    public boolean moveData(String[] hashRange, String targetName){
    	
    	JSONObject jsonMessage;
		try {
			jsonMessage = retrieveZnodeData(targetName);
			
			String targetServerHost = (String) jsonMessage.get("NodeHost");
	        int targetServerPort = Integer.parseInt(jsonMessage.get("NodePort").toString());
	        System.out.println(targetServerHost + " " + targetServerPort);
	    	KVStore migrationClient = new KVStore(targetServerHost, targetServerPort);
	    	migrationClient.connect();
	    	System.out.println("pass migrationClient connect");
	    	migrationClient.enableTransfer();
	    	File[] files = new File(dbPath).listFiles();
	    	System.out.println("size of db: " + files.length);
	        for (File file: files) {
	        	String fileName = file.getName();
	            if (fileName.endsWith(".kv")) {
	            	System.out.println("file in moveData: " + fileName);
	                String key = fileName.substring(0, fileName.length() - 3);
	                String hashedKey = hasher.hashString(key);
	                System.out.println("key in moveData: " + key);
	                System.out.println("hash range in moveData: " + hashRange);
	                if (this.isKeyInRange(key, hashRange[0], hashRange[1])) {
	                	String value = getValueFromFile(dbPath + fileName);
	                	System.out.println("send key: " + key);
	                	migrationClient.put(key, value);
	                }
//	                if ((hasher.compareHash(hashedKey, hashRange[0]) == -1) || (hasher.compareHash(hashedKey, hashRange[0]) == 0)
//	                		|| ((hasher.compareHash(hashedKey, hashRange[0]) == -1) && (hasher.compareHash(hashedKey, hashRange[1]) == 1))
//	                		|| ((hasher.compareHash(hashedKey, hashRange[0]) == -1) && (hasher.compareHash(hashedKey, hashRange[1]) == -1))
//	                		|| ((hasher.compareHash(hashedKey, hashRange[0]) == 1) && (hasher.compareHash(hashedKey, hashRange[1]) == 1))) {
//	                	String value = getValueFromFile(dbPath + fileName);
//                		System.out.println("send key: " + key);
//                		migrationClient.put(key, value);
//	                }
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
    
    private void notifyECS(String target) {
    	JSONObject jsonMessage = new JSONObject();
        jsonMessage.put("NodeName", this.name);
        jsonMessage.put("NodeHost", this.metaDataEntry.serverHost);
        jsonMessage.put("NodePort", Integer.toString(this.metaDataEntry.serverPort));
        jsonMessage.put("CacheStrategy", this.strategy.toString());
        jsonMessage.put("CacheSize", Integer.toString(this.cacheSize));
        jsonMessage.put("State", this.state);
        jsonMessage.put("NodeHash", hasher.hashString(this.name));
        jsonMessage.put("LeftHash", this.metaDataEntry.leftHash);
        jsonMessage.put("RightHash", this.metaDataEntry.rightHash);
        jsonMessage.put("Target", target);
        jsonMessage.put("Transfer", "OFF");
//        System.out.println("in notfiyECS");
//        System.out.println(jsonMessage.toString());
        byte[] zkData = jsonMessage.toString().getBytes();
        try {
//        	this.zk.exists(zkPath, true).getVersion()
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
        running = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + getPort(), e);
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
    
    private void deleteOutOfRangeKey() {
    	File[] files = new File(dbPath).listFiles();
        for (File file: files) {
        	String fileName = file.getName();
            if (fileName.endsWith(".kv")) {
                String key = fileName.substring(0, fileName.length() - 3);
                System.out.println(key + " is deleted");
            	try {
					if (!this.isKeyInRange(key, null, null)) {
						this.deleteKV(key);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
                
            }
        }
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
				System.out.println(transferState);
				if (state.equals("START")) {
					this.start();
				} else if (state.equals("STOP")) {
					this.stop();
				}
				if (transferState.equals("ON")) {
					this.lockWrite();
				} else {
					this.unlockWrite();
				}
				String newLeftHash = (String) jsonMessage.get("LeftHash");
				if (!targetName.equals("null") && transferState.equals("ON")) {
					String[] hashRange = new String[2];
					
//					String newRightHash = (String) jsonMessage.get("RightHash");
					if (newLeftHash.equals("-1")) {
						System.out.println("removeNode case");
						hashRange[0] = this.metaDataEntry.leftHash;
						hashRange[1] = this.metaDataEntry.rightHash;
					} else {
						System.out.println("addeNode case");
						hashRange[0] = this.metaDataEntry.leftHash;
						hashRange[1] = newLeftHash;
					}
					System.out.println(hashRange);
					boolean result;
					try {
						result = this.moveData(hashRange, targetName);
						if (!result)
							System.out.println("data transfer failed");
					} catch (Exception e) {
						e.printStackTrace();
					}
					this.deleteOutOfRangeKey();
//					String time = Long.toString(System.currentTimeMillis());
//			        File timeFile = new File(dbPath + time);
//			        
//			        if (!timeFile.exists()) {
//			            try {
//			            	timeFile.createNewFile();
//			            } catch (IOException e1) {
//			                
//			            }
//			        } 
				}
				if (newLeftHash.equals("-1")) {
					this.stop();
				}
				MetaDataEntry metaDataEntry = this.fillUpMetaDataEntry(jsonMessage);
				this.update(metaDataEntry);
				System.out.println(this.metaDataEntry.leftHash  + " " + this.metaDataEntry.rightHash);
				if (!targetName.equals("null") && transferState.equals("ON")) {
					this.notifyECS(targetName);
				}
				break;
//			case NodeCreated:
//				this.start();
//				break;
			case NodeDeleted:
				this.close();
				break;	
			default:
                break;
		}
		System.out.println("done trigger");
		
	}
}
