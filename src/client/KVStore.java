package client;

import java.net.Socket;
import java.util.HashSet;

import org.apache.log4j.Logger;

import common.messages.KVMessage;

public class KVStore implements KVCommInterface {
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	
	private Socket socket;
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
	        logger.info("Connection established");
		} catch(Exception e) {
			logger.error("Connection could not be established!");
		}
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub
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
