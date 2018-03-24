package client;

import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import common.message.KVMessage;
import common.message.KVMessage.StatusType;
import common.message.TextMessage;
import common.helper.HashComparator;
import common.helper.MD5Hasher;
import common.message.MetaDataEntry;

public class KVStore implements KVCommInterface {
    /**
     * Initialize KVStore with address and port of KVServer
     * @param address the address of the KVServer
     * @param port the port of the KVServer
     */
	
	private String initial_address;
	private int initial_port;
    private TreeMap<String, MetaDataEntry> metaData = new TreeMap<String, MetaDataEntry>();
    private HashMap<AddressPair, Socket> socketMap = new HashMap<AddressPair, Socket>();
    private Logger logger = Logger.getRootLogger();
    private MD5Hasher hasher;
    private HashSet<StatusType> retryStatuses = new HashSet<StatusType>();
    private volatile boolean isTransfer; 
    private volatile boolean isReplicate; 

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
    private static final int RETRY_ATTEMPTS = 20;
    private static final int RETRY_SLEEP_MS = 50;
    
    public KVStore(String input_address, int input_port) throws IOException, NoSuchAlgorithmException {
    	initial_address = input_address;
    	initial_port = input_port;
        hasher = new MD5Hasher();
        retryStatuses.add(StatusType.SERVER_NOT_RESPONSIBLE);
        retryStatuses.add(StatusType.SERVER_STOPPED);
        retryStatuses.add(StatusType.SERVER_WRITE_LOCK);
        isTransfer = false;
    }

    @Override
    public void connect() throws UnknownHostException, IOException {
    	metaData = initializeMetadata(initial_address, initial_port);
        socketMap.put(new AddressPair(initial_address, initial_port), new Socket(initial_address, initial_port));
    }

    @Override
    public void disconnect() {
        logger.info("tearing down all connections ...");
        for (Socket socket : socketMap.values()) {
	        try {
	            if (socket != null) {
	                socket.close();
	                socket = null;
	                logger.info("connection closed!");
	            }
	        } catch(IOException ioe) {
	            logger.error("Unable to close connection!");
	        }
        }
    }

    @Override
    public boolean isConnected() {
        return socketMap.size() > 0;
    }

    @Override
    public KVMessage put(String key, String value) throws Exception {
        // Create Request
        TextMessage req = null;
        if (isTransfer) {
        	req = new TextMessage("TRANSFER", key, value);
        } else if (isReplicate) {
        	req = new TextMessage("REPLICATE", key, value);
        } else {
        	if (value != "" && value != null) {
	            req = new TextMessage("PUT", key, value);
	        } else {
	            req = new TextMessage("DELETE", key, value);
	        }
        }
        return serverRequest(key, req);
    }
    
    public void enableTransfer() {
    	this.isTransfer = true;
    }
    
    public void enableReplicate() {
    	this.isReplicate = true;
    }
    
//    public void disableTransfer() {
//    	this.isTransfer = false;
//    }

    @Override
    public KVMessage get(String key) throws Exception {
        // Create Request
        TextMessage req = new TextMessage("GET", key, "");
        return serverRequest(key, req);
    }
    
    public KVMessage get(String key, String address, int port) throws UnknownHostException, IOException {
    	TextMessage req = new TextMessage("GET", key, "");
    	byte[] req_byte = req.getMsgBytes();
    	Socket socket = socketMap.get(new AddressPair(address, port));
        if (socket == null) {
        	socket = connect(address, port);
        }
        OutputStream output = socket.getOutputStream();
        InputStream input = socket.getInputStream();
        output.write(req_byte, 0, req_byte.length);
        output.flush();
        return receiveMessage(input);
    }
    
    public TreeMap<String, MetaDataEntry> getMetadata() {
    	return metaData;
    }
    
    public HashMap<AddressPair, Socket> getSocketMap() {
    	return socketMap;
    }
    
    private KVMessage serverRequest(String key, TextMessage req) throws Exception {
    	byte[] req_byte = req.getMsgBytes();
        MetaDataEntry metaDataEntry = getResponsibleServer(key, true);
        String address = metaDataEntry.serverHost;
        int port = metaDataEntry.serverPort;
        Socket socket = socketMap.get(new AddressPair(address, port));
        if (socket == null) {
        	socket = connect(address, port);
        }
        OutputStream output = socket.getOutputStream();
        InputStream input = socket.getInputStream();
        int attempts = 0;
        TextMessage res = null;
        while (attempts < RETRY_ATTEMPTS && (res == null || retryStatuses.contains(res.getStatus()))) {
        	attempts += 1;
        	System.out.println(attempts);
        	if (res != null) {
        		Thread.sleep(RETRY_SLEEP_MS);
        		if (res.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
        			System.out.println("NOT RESPON");
        			metaData = buildTreeMap(res.getMetaData());
        			System.out.println("Build TREEMAP");
        			metaDataEntry = getResponsibleServer(key, true);
        			System.out.println(metaDataEntry.serverPort);
                    address = metaDataEntry.serverHost;
                    port = metaDataEntry.serverPort;
                    socket = socketMap.get(new AddressPair(address, port));
                    System.out.println("Got socket");
                    if (socket == null) {
                    	socket = connect(address, port);
                    }
                    System.out.println("Getting streams");
                    output = socket.getOutputStream();
                    input = socket.getInputStream();
                    System.out.println("got streams");
        		}
        	}
        	System.out.println("Before OUTPUT");
        	try {
	            output.write(req_byte, 0, req_byte.length);
	            output.flush();
        	} catch (IOException e) {
        		// Try to handle case where cached responsible server is down.
        		metaDataEntry = getResponsibleServer(metaDataEntry.rightHash, false);
                address = metaDataEntry.serverHost;
                port = metaDataEntry.serverPort;
                socket = socketMap.get(new AddressPair(address, port));
                if (socket == null) {
                	socket = connect(address, port);
                }
                output = socket.getOutputStream();
                input = socket.getInputStream();
                output.write(req_byte, 0, req_byte.length);
                output.flush();
        	}
            System.out.println("Before RECEIVE");
            res = receiveMessage(input);
            System.out.println("After RECEIVE");
        }
        if (retryStatuses.contains(res.getStatus())) {
        	throw new TimeoutException("Timed out after retrying server requests.");
        }
        return res;
    }
    
    private Socket connect(String address, int port) throws UnknownHostException, IOException {
    	Socket socket = new Socket(address, port);
    	socketMap.put(new AddressPair(address, port), socket);
    	return socket;
    }
    
    public MetaDataEntry getResponsibleServer(String key, boolean allowEqual) {
    	String hashCode = hasher.hashString(key);
    	Map.Entry<String, MetaDataEntry> entry = metaData.ceilingEntry(hashCode);
    	if (!allowEqual) {
    		entry = metaData.higherEntry(hashCode);
    	}
    	MetaDataEntry responsible = null;
    	if (entry == null) {
    		responsible = metaData.ceilingEntry("0").getValue();
    	} else {
    		responsible = entry.getValue();
    	}
    	return responsible;
    }
    
    public TreeMap<String, MetaDataEntry> initializeMetadata(String address, int port) {
    	TreeMap<String, MetaDataEntry> metaData = new TreeMap<String, MetaDataEntry>(new HashComparator());
    	String leftHash = "0";
    	String rightHash = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";
    	MetaDataEntry entry = new MetaDataEntry("InitialServer", address, port, leftHash, rightHash);
    	metaData.put(rightHash, entry);
    	return metaData;
    }
    
    public TreeMap<String, MetaDataEntry> buildTreeMap(List<MetaDataEntry> metaDataList) {
    	TreeMap<String, MetaDataEntry> metaData = new TreeMap<String, MetaDataEntry>(new HashComparator());
    	for (MetaDataEntry entry : metaDataList) {
    		this.metaData.remove(entry.rightHash);
    		metaData.put(entry.rightHash, entry);
    	}
    	// Close connections to servers that are no longer running.
    	for (Map.Entry<String, MetaDataEntry> entry : this.metaData.entrySet()) {
    		AddressPair removedServerAddr = new AddressPair(entry.getValue().serverHost, entry.getValue().serverPort);
    		Socket removedServerConn = socketMap.get(removedServerAddr);
    		if (removedServerConn != null) {
	    		try {
	    			removedServerConn.close();
	    		} catch(IOException e) {
	    			logger.error("Unable to close connection");
	    		}
	    		socketMap.remove(removedServerAddr);
    		}
    	}
    	return metaData;
    }

    private TextMessage receiveMessage(InputStream input) throws IOException {
        int index = 0;
        byte[] msgBytes = null, tmp = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];
        
        /* read first char from stream */
        byte read = (byte) input.read();    
        boolean reading = true;
        
        while(read != 13 && reading) {/* carriage return */
            /* if buffer filled, copy to msg array */
            if(index == BUFFER_SIZE) {
                if(msgBytes == null){
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            } 
            
            /* only read valid characters, i.e. letters and numbers */
            if((read > 31 && read < 127)) {
                bufferBytes[index] = read;
                index++;
            }
            
            /* stop reading is DROP_SIZE is reached */
            if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }
            
            /* read next char from stream */
            read = (byte) input.read();
        }
        
        if(msgBytes == null){
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }
        
        msgBytes = tmp;
        
        /* build final String */
        TextMessage msg = new TextMessage(msgBytes);
        logger.info("Receive message:\t '" + msg.getMsg() + "'");
        return msg;
    }
}
