package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import client.KVCommInterface;

public class KVClient implements IKVClient {

	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "KVClient> ";
	private BufferedReader stdin;
	private boolean stop = false;

	private String serverAddress;
	private int serverPort;

    @Override
    public void newConnection(String hostname, int port) throws Exception{
        // TODO Auto-generated method stub
    }

    @Override
    public KVCommInterface getStore(){
        // TODO Auto-generated method stub
        return null;
    }
  
	public void run() {
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}
	}

	private void handleCommand(String cmdLine) {
		String[] tokens = cmdLine.split("\\s+");

		if(tokens[0].equals("quit")) {
			/*
			stop = true;
			disconnect();
			System.out.println(PROMPT + "Application exit!");
			*/
		
		} else if (tokens[0].equals("connect")){
			/*
			if(tokens.length == 3) {
				try{
					serverAddress = tokens[1];
					serverPort = Integer.parseInt(tokens[2]);
					connect(serverAddress, serverPort);
				} catch(NumberFormatException nfe) {
					printError("No valid address. Port must be a number!");
					logger.info("Unable to parse argument <port>", nfe);
				} catch (UnknownHostException e) {
					printError("Unknown Host!");
					logger.info("Unknown Host!", e);
				} catch (IOException e) {
					printError("Could not establish connection!");
					logger.warn("Could not establish connection!", e);
				}
			} else {
				printError("Invalid number of parameters!");
			}
			*/
		} else  if (tokens[0].equals("send")) {
			/*
			if(tokens.length >= 2) {
				if(client != null && client.isRunning()){
					StringBuilder msg = new StringBuilder();
					for(int i = 1; i < tokens.length; i++) {
						msg.append(tokens[i]);
						if (i != tokens.length -1 ) {
							msg.append(" ");
						}
					}	
					sendMessage(msg.toString());
				} else {
					printError("Not connected!");
				}
			} else {
				printError("No message passed!");
			}
			*/
			
		} else if(tokens[0].equals("disconnect")) {
			/*
			disconnect();
			*/
			
		} else if(tokens[0].equals("logLevel")) {
			/*
			if(tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + 
							"Log level changed to level " + level);
				}
			} else {
				printError("Invalid number of parameters!");
			}
			*/
			
		} else if(tokens[0].equals("help")) {
			printHelp();
		} else {
			printError("Unknown command");
			printHelp();
		}
	}

	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("KV CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t\t establishes a connection to a server\n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");
		sb.append(PROMPT).append("put <key> <value>");
		sb.append("\t\t Store a KV pair on the server \n");
		sb.append(PROMPT).append("get <key>");
		sb.append("\t\t\t Retrieve a KV pair on the server \n");
		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t\t exits the program");
		System.out.println(sb.toString());
	}

	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}

    public static void main(String[] args) {
    	try {
			new LogSetup("logs/client.log", Level.OFF);
			KVClient app = new KVClient();
			app.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
    }
}
