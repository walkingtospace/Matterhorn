package app_kvClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import client.KVCommInterface;
import client.KVClientSocketListener.SocketStatus;

public class KVClient extends Thread implements IKVClient {

    private Logger logger = Logger.getRootLogger();
    private Set<KVClientSocketListener> listeners;
    private boolean running;
    
    private Socket clientSocket;
    private OutputStream output;
    private InputStream input;
    
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

    public KVClient(String address, int port) 
            throws UnknownHostException, IOException {
        newConnection(address, port);
    }

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        clientSocket = new Socket(address, port);
        listeners = new HashSet<KVClientSocketListener>();
        setRunning(true);
        logger.info("Connection established");
    }

    @Override
    public KVCommInterface getStore() {
        // Get the communication module
        return null;
    }

    public void run() {
        try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();
            
            while(isRunning()) {
                try {
                    TextMessage latestMsg = receiveMessage();
                    for(ClientSocketListener listener : listeners) {
                        listener.handleNewMessage(latestMsg);
                    }
                } catch (IOException ioe) {
                    if(isRunning()) {
                        logger.error("Connection lost!");
                        try {
                            tearDownConnection();
                            for(ClientSocketListener listener : listeners) {
                                listener.handleStatus(
                                        SocketStatus.CONNECTION_LOST);
                            }
                        } catch (IOException e) {
                            logger.error("Unable to close connection!");
                        }
                    }
                }               
            }
        } catch (IOException ioe) {
            logger.error("Connection could not be established!");
            
        } finally {
            if(isRunning()) {
                closeConnection();
            }
        }
    }

    public void closeConnection() {
        logger.info("try to close connection ...");
        try {
            tearDownConnection();
            for(ClientSocketListener listener : listeners) {
                listener.handleStatus(SocketStatus.DISCONNECTED);
            }
        } catch (IOException ioe) {
            logger.error("Unable to close connection!");
        }
    }

    public boolean isRunning() {
        return running;
    }
    
    public void setRunning(boolean run) {
        running = run;
    }

    public void addListener(ClientSocketListener listener){
        listeners.add(listener);
    }

    private void tearDownConnection() throws IOException {
        setRunning(false);
        logger.info("tearing down the connection ...");
        if (clientSocket != null) {
            //input.close();
            //output.close();
            clientSocket.close();
            clientSocket = null;
            logger.info("connection closed!");
        }
    }

}
