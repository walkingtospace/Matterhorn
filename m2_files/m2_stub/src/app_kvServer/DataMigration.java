package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import common.message.TextMessage;
import common.message.KVMessage.StatusType;

public class DataMigration implements Runnable {

private static Logger logger = Logger.getRootLogger();
    
    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;
    
    private Socket clientSocket;
    private InputStream input;
    private OutputStream output;
    
    private KVServer kvServer;
    
    /**
     * Constructs a new CientConnection object for a given TCP socket.
     * @param clientSocket the Socket object for the client connection.
     */
    public DataMigration(Socket clientSocket, KVServer kvServer) {
        this.clientSocket = clientSocket;
        this.kvServer = kvServer;
        this.isOpen = true;
    }
	
	@Override
	public void run() {
		try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();
            
            while(isOpen) {
                try {
                    TextMessage latestMsg = receiveMessage();
                    if (latestMsg.getMsg().trim().length() == 0) {
                        throw new IOException();
                    }
                    StatusType operation = latestMsg.getStatus();
                    
                    String key = null;
                    String value = null;
                    StatusType status = StatusType.PUT_SUCCESS;
                    switch (operation) {
                        case PUT:
                            key = latestMsg.getKey();
                            value = latestMsg.getValue();
                            if (key.isEmpty() || key.contains(" ") || key.length() > 20 || value.length() > 120000) {
                                logger.error("Error! Unable to PUT due to invalid key or value!");
                                status = StatusType.PUT_ERROR;
                                break;
                            }
                            try {
                                if (kvServer.inStorage(key))
                                    status = StatusType.PUT_UPDATE;
                                kvServer.putKV(key, value);
                            } catch (Exception e) {
                                logger.error("Error! Unable to PUT key-value pair!", e);
                                status = StatusType.PUT_ERROR;
                            }
                            break;
                        case GET:
                            key = latestMsg.getKey();
                            if (key.isEmpty() || key.contains(" ") || key.length() > 20) {
                                logger.error("Error! Unable to GET due to invalid key");
                                status = StatusType.GET_ERROR;
                                break;
                            }
                            try {
                                value = kvServer.getKV(latestMsg.getKey());
                                if (value == null)
                                    status = StatusType.GET_ERROR;
                                else
                                    status = StatusType.GET_SUCCESS;
                            } catch (Exception e) {
                                logger.error("Error! Unable to GET key-value pair!", e);
                                status = StatusType.GET_ERROR;
                            }
                            break;
                        case DELETE:
                            key = latestMsg.getKey();
                            value = "";
                            try {
                            	boolean result = kvServer.deleteKV(key);
                                status = result ? StatusType.DELETE_SUCCESS : StatusType.DELETE_ERROR;
                            } catch (Exception e) {
                                logger.error("Error! Unable to DELETE key-value pair!", e);
                                status = StatusType.DELETE_ERROR;
                            }
                            break;
                        default:
                            break;
                    }
                    TextMessage resultMsg = new TextMessage(status, key, value);
                    
                    sendMessage(resultMsg);
                    
                /* connection either terminated by the client or lost due to 
                 * network problems*/   
                } catch (IOException ioe) {
                    logger.error("Error! Connection lost!");
                    isOpen = false;
                }               
            }
            
        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);
            
        } finally {
            
            try {
                if (clientSocket != null) {
                    input.close();
                    output.close();
                    clientSocket.close();
                }
            } catch (IOException ioe) {
                logger.error("Error! Unable to tear down connection!", ioe);
            }
        }
		
	}

}
