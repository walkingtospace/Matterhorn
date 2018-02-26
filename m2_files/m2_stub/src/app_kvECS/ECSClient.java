package app_kvECS;

// Java Import
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Collection;
import java.lang.Integer;

// 3rd party library import
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

// Internal import
import logger.LogSetup;

// ECS import
import ecs.IECSNode;
import ecs.ECS;

public class ECSClient implements IECSClient {

    private static Logger logger = Logger.getRootLogger();
    private BufferedReader stdin;
    private ECS ecs;
    private static final String PROMPT = "ECSClient> ";
    private static final String CONFIGPATH = "esc.config";


    public ECSClient() {
        ecs = ECS(CONFIGPATH);
    }

    public void run() {
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);
            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                printError("CLI does not respond - Application terminated ");
            }
        }
    }


    public void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");

        if(tokens[0].equals("quit")) {
            printInfo("Application exit!");
        } else if (tokens[0].equals("addNodes")){
            this.addNodes(Integer.parseInt(tokens[1]), tokens[2],
                          Integer.parseInt(tokens[3]));
        } else if (tokens[0].equals("start")) {
            this.start();
        } else if (tokens[0].equals("stop")) {
            this.stop();
        } else if(tokens[0].equals("shutdown")) {
            this.shutdown();
        } else if(tokens[0].equals("addNode")) {
            this.addNode(tokens[1], Integer.parseInt(tokens[2]))
        } else if(tokens[0].equals("removeNode")) {
            this.removeNode(tokens[1:]);
        } else if(tokens[0].equals("logLevel")) {
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
        } else if(tokens[0].equals("help")) {
            printHelp();
        } else {
            printError("Unknown command");
            printHelp();
        }
    }


    @Override
    public boolean start() {
        return esc.start();
    }


    @Override
    public boolean stop() {
        return ecs.stop();
    }


    @Override
    public boolean shutdown() {
        return ecs.shutdown();
    }


    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        return esc.addNode(cacheStrategy, cacheSize);
    }


    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        return esc.addNodes(count, cacheStrategy, cacheSize);
    }


    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        return esc.setupNodes(count, cacheStrategy, cacheSize);
    }


    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        return esc.awaitNodes(count, timeout);
    }


    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        return esc.removeNodes(nodeNames);
    }


    @Override
    public Map<String, IECSNode> getNodes() {
        return esc.getNodes();
    }


    @Override
    public IECSNode getNodeByKey(String Key) {
        return esc.getNodeByKey(key);
    }


    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECSCLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        System.out.println(sb.toString());
    }


    private void printPossibleLogLevels() {
        System.out.println(PROMPT 
                + "Possible log levels are:");
        System.out.println(PROMPT 
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }


    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }


    private void printInfo(String info){
        System.out.println(PROMPT + " " + info);
    }


    private String setLevel(String levelString) {
        if(levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if(levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if(levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if(levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if(levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if(levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if(levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }


    public static void main(String[] args) {
        try {
            new LogSetup("logs/esc_client_ui.log", Level.OFF);
            ECSClient escclient_ui = new ECSClient();
            escclient_ui.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
