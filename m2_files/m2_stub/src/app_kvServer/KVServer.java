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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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
import logger.LogSetup;

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
    private boolean writeLock;
    
    private volatile String state;
    
    private ZooKeeper zk;
    private MD5Hasher hasher;

    public KVServer(String name,
                    String zkHostname,
                    int zkPort) throws KeeperException, InterruptedException, IOException {
        /* KenNote: The func signature is different in M2 */
    	this.name = name;
        this.dbPath += name + "/";
        this.zkHostname = zkHostname;
        this.zkPort = zkPort;
        
        try {
			hasher = new MD5Hasher();
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
        
        String connection = zkHostname + ":" + Integer.toString(zkPort) + zkPath + name;
        zk = new ZooKeeper(connection, 3000, this);
    	
        byte[] raw_data = zk.getData(zkPath + name, this, null);
        String data = new String(raw_data);
        
        JSONObject jsonMessage = decodeJsonStr(data);
        
        state = (String) jsonMessage.get("State");
        int cacheSize = Integer.parseInt((String)jsonMessage.get("CacheSize"));
        String strategy = (String)jsonMessage.get("CacheStrategy");
        String serverHost = (String)jsonMessage.get("NodeHost");
        int serverPort = Integer.parseInt((String)jsonMessage.get("NodePort"));
        String leftHash = (String)jsonMessage.get("LeftHash");
        String rightHash = (String)jsonMessage.get("RightHash");


        MetaDataEntry metaDataEntry = new MetaDataEntry(name, serverHost, serverPort, leftHash, rightHash);
        
        this.initKVServer(metaDataEntry, cacheSize, strategy);
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
    
    
    public synchronized void initKVServer(MetaDataEntry metaDataEntry, int cacheSize, String strategy) throws KeeperException, InterruptedException {
    	
    	this.metaDataEntry = metaDataEntry;
    	this.cacheSize = cacheSize;
        this.strategy = CacheStrategy.valueOf(strategy);
        this.cache = createCache(this.strategy);
        
        File dbDir = new File(dbPath);
        try{
            dbDir.mkdir();
        }
        catch(SecurityException se){
            logger.error("Error! Can't create database folder");
        }
        
        
        
        running = initializeServer();
        if(serverSocket != null) {
            while(running){
                try {
                    Socket client = serverSocket.accept();                
                    ClientConnection connection = new ClientConnection(client, this);
                    new Thread(connection).start();
                    
                    logger.info("Connected to " 
                            + client.getInetAddress().getHostName() 
                            +  " on port " + client.getPort());
                } catch (IOException e) {
                    logger.error("Error! " +
                            "Unable to establish connection. \n", e);
                }
            }
        }
        logger.info("Server stopped.");
    }
    
    public List<MetaDataEntry> fillMetaData() throws IOException, KeeperException, InterruptedException {
    	List<MetaDataEntry> metaData = new ArrayList<>();
    	String connection = this.zkHostname + ":" + Integer.toString(this.zkPort) + zkPath;
    	ZooKeeper rootZk = new ZooKeeper(connection, 3000, null);
    	List<String> zNodes = rootZk.getChildren(zkPath, false);
    	for (String zNode: zNodes) {

    		byte[] raw_data = rootZk.getData(zkPath + zNode, false, null);
            String data = new String(raw_data);
            
            JSONObject jsonMessage = decodeJsonStr(data);
            
            String serverHost = (String)jsonMessage.get("NodeHost");
            int serverPort = Integer.parseInt((String)jsonMessage.get("NodePort"));
            String leftHash = (String)jsonMessage.get("LeftHash");
            String rightHash = (String)jsonMessage.get("RightHash");
            
            MetaDataEntry nodeMetaDataEntry = new MetaDataEntry(serverHost, serverHost, serverPort, leftHash, rightHash);
            metaData.add(nodeMetaDataEntry);
    	}
    	return metaData;
    }
    
    // invoker should provide zkHostname, zkPort, name and zkPath
    public static void main(String[] args) {
		try {
//			if (args.length != 4) {
//				System.out.println("Wrong number of arguments passed to server");
//			}
//			new LogSetup("logs/server.log", Level.ALL);
//			String zkHostname = args[0];
//			int zkPort = Integer.parseInt(args[1]);
//			String name = args[2];
			String name = "server1";
			String zkHostname = "0.0.0.0";
			int zKPort = 3100;
			KVServer server = new KVServer(name, zkHostname, zKPort);
			
		} catch(Exception e) {
			logger.error("Error! Can't start server");
		}
	}
    

    
    public void test() {
    	
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

    public synchronized String getState() {
    	return this.state;
    }
    
    @Override
    public synchronized boolean inStorage(String key){
        boolean result = false;
        key += ".kv";
        File kvFile = new File(dbPath + key);
        if (kvFile.exists()) {
            result = true;
        }
        return result;
    }


    @Override
    public synchronized boolean inCache(String key){
        if (cache != null && cache.get(key) != null) {
            return true;
        }
        return false;
    }


    @Override
    public synchronized String getKV(String key) throws Exception{
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

    public boolean isKeyInRange(String key) throws NoSuchAlgorithmException {
    	
    	String hashedKey = hasher.hashString(key);
    	String leftHash = metaDataEntry.leftHash;
    	String rightHash = metaDataEntry.rightHash;
    	if ((hasher.compareHash(leftHash, hashedKey) == -1 && hasher.compareHash(rightHash, hashedKey) == 1) 
    			|| (hasher.compareHash(hashedKey, leftHash) == -1 && hasher.compareHash(hashedKey, rightHash) == -1)
    			|| (hasher.compareHash(hashedKey, leftHash) == 1 && hasher.compareHash(hashedKey, rightHash) == 1)) {
    		return true;
    	}
    	return false;
    }

    @Override
    public synchronized void putKV(String key, String value) throws Exception{
    	if (writeLock == true)
    		return;
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
	public synchronized boolean deleteKV(String key) throws Exception{
    	if (writeLock == true)
    		return false;
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
    public synchronized void clearCache(){
        /* KenNote: Just copied over from M1 */
        cache = createCache(strategy);
    }


    @Override
    public synchronized void clearStorage(){
        /* KenNote: Just copied over from M1 */
        File[] files = new File(dbPath).listFiles();
        for (File file: files) {
            if (file.toString().endsWith(".kv")) {
                file.delete();
            }
        }
    }


    @Override
    public void run(){
        /* KenNote: Just copied over from M1 */
        running = initializeServer();
        
        if(serverSocket != null) {
            while(running){
                try {
                    Socket client = serverSocket.accept();                
                    ClientConnection connection = new ClientConnection(client, this);
                    new Thread(connection).start();
                    
                    logger.info("Connected to " 
                            + client.getInetAddress().getHostName() 
                            +  " on port " + client.getPort());
                } catch (IOException e) {
                    logger.error("Error! " +
                            "Unable to establish connection. \n", e);
                }
            }
        }
        logger.info("Server stopped.");
    }


    @Override
    public void kill(){
        /* KenNote: Just copied over from M1 */
        stopServer();
    }


    @Override
    public void close(){
        /* KenNote: Just copied over from M1 */
        // Cache is write-through, so saving is not necessary.
        stopServer();
    }


    @Override
    public synchronized void start() {
        this.state = "START";
    }


    @Override
    public void stop() {
    	this.state = "STOP";
    }


    @Override
    public synchronized void lockWrite() {
    	writeLock = true;
    }


    @Override
    public synchronized void unlockWrite() {
    	writeLock = false;
    }
    
    public synchronized boolean isLocked() {
    	return writeLock;
    }
    
    @Override
    public synchronized void update(MetaDataEntry metaDataEntry) {
    	this.metaDataEntry = metaDataEntry;
    }


    @Override
    // assume hashRange is given in clockwise rotation
    public boolean moveData(String[] hashRange, String targetName) throws Exception {

    	// logic to retrieve MetaData
    	byte[] raw_data = zk.getData(zkPath + targetName, this, null);
    	String data = new String(raw_data);
    	
    	JSONObject jsonMessage = null;
        JSONParser parser = new JSONParser();
        try {
            jsonMessage = (JSONObject) parser.parse(data);
        } catch (ParseException e) {
            logger.error("Error! " +
                    "Unable to parse incoming bytes to json. \n", e);
        }
        String targetServerHost = (String) jsonMessage.get("NodeHost");
        int targetServerPort = Integer.parseInt((String)jsonMessage.get("l"));
    	
//    	List<String> list = zk.getChildren(zkPath, true);
    	
    	KVStore migrationClient = new KVStore(targetServerHost, targetServerPort);
    	
    	migrationClient.connect();

//    	String maxHash = hasher.hashString("0");
//    	String minHash = hasher.hashString("FFFFFF");
    	File[] files = new File(dbPath).listFiles();
        for (File file: files) {
        	String fileName = file.toString();
            if (fileName.endsWith(".kv")) {
                String key = fileName.substring(0, fileName.length() - 3);
                String hashValue = hasher.hashString(key);
                
                if ((hasher.compareHash(hashRange[0], hashValue) == -1 && hasher.compareHash(hashValue, hashRange[1]) == -1) 
                		|| (hasher.compareHash(hashRange[0], hashValue) == 0)) {
                	String value = getValueFromFile(dbPath + fileName);
                	migrationClient.put(key, value);
                // special case: hashRange across starting of the ring or end of the ring
                } else if ((hasher.compareHash(hashValue, hashRange[0]) == -1 && hasher.compareHash(hashValue, hashRange[1]) == -1) || 
                		(hasher.compareHash(hashValue, hashRange[0]) == 1 && hasher.compareHash(hashValue, hashRange[1]) == 1)) {
                	String value = getValueFromFile(dbPath + fileName);
                	migrationClient.put(key, value);
                }
            }
        }
        migrationClient.disconnect();
        return true;
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
        logger.info("Initialize server ...");
        try {
            serverSocket = new ServerSocket(getPort());
            logger.info("Server listening on port: " 
                    + serverSocket.getLocalPort());    
            return true;
        
        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
                logger.error("Port " + getPort() + " is already bound!");
            }
            return false;
        }
    }

	@Override
	public void process(WatchedEvent event) {
		byte[] raw_data;
		try {
			raw_data = zk.getData(zkPath, this, null);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
			return;
		}
		String data = new String (raw_data);
		
		JSONObject jsonMessage = null;
        JSONParser parser = new JSONParser();
        try {
            jsonMessage = (JSONObject) parser.parse(data);
        } catch (ParseException e) {
            logger.error("Error! " +
                    "Unable to parse incoming bytes to json. \n", e);
        }
        
		EventType type = event.getType();
		switch (type) {
			case NodeDataChanged:
				if (event.getPath().equals(this.zkPath + this.name)) {
					String targetName = (String) jsonMessage.get("TargetName");
					String leftHash = (String) jsonMessage.get("LeftHash");
					String rightHash = (String) jsonMessage.get("RighttHash");
					if (targetName.equals("NULL")) {
						String[] hashRange = {leftHash, rightHash};
						boolean result;
						try {
							this.lockWrite();
							result = this.moveData(hashRange, targetName);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							return;
						}
						if (result) {
							MetaDataEntry metaDataEntry = new MetaDataEntry(this.name, this.metaDataEntry.serverHost, this.metaDataEntry.serverPort, leftHash, rightHash);
							this.update(metaDataEntry);
							this.unlockWrite();
						}
					}
//					if (hasher.compareHash(leftHash, metaDataEntry.leftHash) != 0 || hasher.compareHash(rightHash, metaDataEntry.rightHash) != 0) {
//						
//					}
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
		
	}
}
