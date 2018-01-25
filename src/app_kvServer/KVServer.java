package app_kvServer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.*;


import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvClient.KVClient;

public class KVServer implements IKVServer {

	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache
	 *           is full and there is a GET- or PUT-request on a key that is
	 *           currently not contained in the cache. Options are "FIFO", "LRU",
	 *           and "LFU".
	 */
	private static Logger logger = Logger.getRootLogger();
	private boolean running;
	private ServerSocket serverSocket;
	
	private int port;
	private int cacheSize;
	private CacheStrategy strategy;
	
	private String dbPath = "db.txt";
	
	public KVServer(int port, int cacheSize, String strategy) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = CacheStrategy.valueOf(strategy);
		this.run();
	}

	@Override
	public int getPort(){
		return port;
	}

	@Override
    public String getHostname(){
		try {
			return InetAddress.getLocalHost().toString();
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
    public CacheStrategy getCacheStrategy(){
		return strategy;
	}

	@Override
    public int getCacheSize(){
		return cacheSize;
	}

	@Override
    public synchronized boolean inStorage(String key){
		String line = null;
		boolean result = false;;
		try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = 
                new FileReader(dbPath);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = 
                new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                if (line.split(" ")[0].equals(key)) {
                	result = true;
                	break;
                }
            }   

            // Always close files.
            bufferedReader.close();         
        }
        catch(FileNotFoundException ex) {
        	logger.error("Error! " +
                "Unable to open database file '" + 
                dbPath + "'");                
        }
        catch(IOException ex) {
        	logger.error(
                "Error! reading file '" 
                + dbPath + "'");                  
        }
		finally {
			return result;
		}
	}

	@Override
    public boolean inCache(String key){
		// TODO Auto-generated method stub
		return false;
	}

	@Override
    public synchronized String getKV(String key) throws Exception{
		String line = null;
		String value = null;
		try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = 
                new FileReader(dbPath);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = 
                new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
            	String[] pair = line.split(" ");
                if (pair[0].equals(key)) {
                	value = pair[1];
                	break;
                }
            }   

            // Always close files.
            bufferedReader.close();         
        }
        catch(FileNotFoundException ex) {
        	logger.error("Error! " +
                "Unable to open database file '" + 
                dbPath + "'");                
        }
        catch(IOException ex) {
        	logger.error(
                "Error! reading file '" 
                + dbPath + "'");                  
        }
		finally {
			return value;
		}
	}

	@Override
    public synchronized void putKV(String key, String value) throws Exception{
		String oldContent = "";
		String line = null;
		int mode = 1; // mode: 1 -> rewrite database; 0 -> append database
		try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader = 
                new FileReader(dbPath);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
            	String[] pair = line.split(" ");
                if (pair[0].equals(key)) {
                	if (pair[1].equals(value)) {
                		bufferedReader.close();
                		return;
                	} else {
                		oldContent += key + " " + value + System.lineSeparator();
                		mode = 0;
                	}
                } else {
                	oldContent += line + System.lineSeparator();
                }
            }
            bufferedReader.close();
            FileWriter fileWriter;
            BufferedWriter bufferedWriter;
            String newContent;
            if (mode == 0) {
            	fileWriter = new FileWriter(dbPath);
            	newContent = oldContent;
                
            } else {
            	fileWriter = new FileWriter(dbPath, true);
                newContent = key + " " + value + System.lineSeparator();
            }
            bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(newContent);
            bufferedWriter.close();
            
        }
        catch(FileNotFoundException ex) {
        	logger.error("Error! " +
                "Unable to open database file '" + 
                dbPath + "'");                
        }
        catch(IOException ex) {
        	logger.error(
                "Error! reading file '" 
                + dbPath + "'");                  
        }
	}

	@Override
    public void clearCache(){
		// TODO Auto-generated method stub
	}

	@Override
    public synchronized void clearStorage(){
		try {
			PrintWriter writer = new PrintWriter(dbPath);
			writer.print("");
			writer.close();
		} catch (FileNotFoundException e) {
			logger.error("Error! " +
	                "Unable to open database file '" + 
	                dbPath + "'");
		}	
	}

	@Override
    public void run(){
		running = initializeServer();
        
        if(serverSocket != null) {
	        while(running){
	            try {
	                Socket client = serverSocket.accept();                
	                ClientConnection connection = new ClientConnection(client);
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
		// TODO Auto-generated method stub
	}

	@Override
    public void close(){
		running = false;
        try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}
	
	private boolean initializeServer() {
    	logger.info("Initialize server ...");
    	try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: " 
            		+ serverSocket.getLocalPort());    
            return true;
        
        } catch (IOException e) {
        	logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
            	logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }
}
