package client;

import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import common.message.KVMessage;
import common.message.KVMessage.StatusType;
import common.message.TextMessage;

import common.helper.MD5Hasher;
import common.message.MetaDataEntry;

public class KVStore implements KVCommInterface {
    /**
     * Initialize KVStore with address and port of KVServer
     * @param address the address of the KVServer
     * @param port the port of the KVServer
     */
	
    private TreeMap<String, MetaDataEntry> metaData;
    private HashMap<AddressKey, Socket> socketMap;
    private Logger logger = Logger.getRootLogger();
    private MD5Hasher hasher;
    private HashSet<StatusType> retryStatuses;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
    private static final int RETRY_ATTEMPTS = 20;
    private static final int RETRY_SLEEP_MS = 50;
    
    public KVStore(String input_address, int input_port) throws IOException, NoSuchAlgorithmException {
        metaData = initializeMetadata(input_address, input_port);
        socketMap.put(new AddressKey(input_address, input_port), new Socket(input_address, input_port));
        hasher = new MD5Hasher();
        retryStatuses = new HashSet<StatusType>();
        retryStatuses.add(StatusType.SERVER_NOT_RESPONSIBLE);
        retryStatuses.add(StatusType.SERVER_STOPPED);
        retryStatuses.add(StatusType.SERVER_WRITE_LOCK);
    }

    @Override
    public void connect() throws UnknownHostException, IOException {
        logger.info("Connect is delayed until attempted operation.");
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
        /* KenNote: New function that needs to be implemented */
        return false;
    }

    @Override
    public KVMessage put(String key, String value) throws Exception {
        // Create Request
        TextMessage req = null;
        if (value != "" && value != null) {
            req = new TextMessage("PUT", key, value);
        } else {
            req = new TextMessage("DELETE", key, value);
        }
        return serverRequest(key, req);
    }

    @Override
    public KVMessage get(String key) throws Exception {
        // Create Request
        TextMessage req = new TextMessage("GET", key, "");
        return serverRequest(key, req);
    }
    
    private KVMessage serverRequest(String key, TextMessage req) throws Exception {
    	byte[] req_byte = req.getMsgBytes();
        MetaDataEntry metaDataEntry = getResponsibleServer(key);
        String address = metaDataEntry.serverHost;
        int port = metaDataEntry.serverPort;
        Socket socket = socketMap.get(new AddressKey(address, port));
        if (socket == null) {
        	socket = connect(address, port);
        }
        OutputStream output = socket.getOutputStream();
        InputStream input = socket.getInputStream();
        int attempts = 0;
        TextMessage res = null;
        while (attempts < RETRY_ATTEMPTS && (res == null || retryStatuses.contains(res.getStatus()))) {
        	attempts += 1;
        	if (res != null) {
        		Thread.sleep(RETRY_SLEEP_MS);
        		if (res.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
        			// metadata = res.getMetaData();
        			metaDataEntry = getResponsibleServer(key);
                    address = metaDataEntry.serverHost;
                    port = metaDataEntry.serverPort;
                    socket = socketMap.get(new AddressKey(address, port));
                    if (socket == null) {
                    	socket = connect(address, port);
                    }
                    output = socket.getOutputStream();
                    input = socket.getInputStream();
        		}
        	}
            output.write(req_byte, 0, req_byte.length);
            output.flush();
            res = receiveMessage(input);
        }
        if (retryStatuses.contains(res.getStatus())) {
        	throw new TimeoutException("Timed out after retrying server requests.");
        }
        return res;
    }
    
    private Socket connect(String address, int port) throws UnknownHostException, IOException {
    	Socket socket = new Socket(address, port);
    	socketMap.put(new AddressKey(address, port), socket);
    	return socket;
    }
    
    private MetaDataEntry getResponsibleServer(String key) {
    	String hashCode = hasher.hashString(key);
    	return metaData.ceilingEntry(hashCode).getValue();
    }
    
    private TreeMap<String, MetaDataEntry> initializeMetadata(String address, int port) {
    	TreeMap<String, MetaDataEntry> metaData = new TreeMap<String, MetaDataEntry>();
    	String leftHash = "0";
    	String rightHash = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";
    	MetaDataEntry entry = new MetaDataEntry("InitialServer", address, port, leftHash, rightHash);
    	metaData.put(rightHash, entry);
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
