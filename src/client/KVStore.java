package client;

import java.net.Socket;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import common.messages.KVMessage;

public class KVStore implements KVCommInterface {
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	
	private Socket socket;
	private OutputStream output;
 	private InputStream input;
	private String address;
	private int port;
    private Logger logger = Logger.getRootLogger();

	
	public KVStore(String input_address, int input_port) {
		address = input_address;
		port = input_port;
    }

	@Override
	public void connect() throws Exception {
		try {
	    	socket = new Socket(address, port);
			output = socket.getOutputStream();
			input = socket.getInputStream();
	        logger.info("Connection established");
		} catch(Exception e) {
			logger.error("Connection could not be established!");
		}
	}

	@Override
	public void disconnect() {
		logger.info("tearing down the connection ...");
		try {
			if (socket != null) {
				//input.close();
				//output.close();
				socket.close();
				socket = null;
				logger.info("connection closed!");
			}
		} catch(IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
}
