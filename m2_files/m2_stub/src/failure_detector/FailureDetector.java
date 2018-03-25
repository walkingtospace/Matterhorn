package failure_detector;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import client.AddressPair;
import client.KVStore;

public class FailureDetector implements Watcher{
	private int intervalSeconds = 30;
	private String zkHostname;
	private int zkPort;
	
	public FailureDetector(int intervalSeconds, String zkHostname, int zkPort) {
		this.intervalSeconds = intervalSeconds;
		this.zkHostname = zkHostname;
		this.zkPort = zkPort;
	}
	
	private void detect() throws InterruptedException, IOException, KeeperException, NoSuchAlgorithmException {
		while(true) {
			List<String> failedServers = checkServerConnections();
			if (!notifyZookeeper(failedServers)) {
				System.out.println("Failed to notify zookeeper about server crashes!");
			}
			Thread.sleep(intervalSeconds * 1000);
		}
	}
	
	private List<String> checkServerConnections() throws IOException, KeeperException, InterruptedException, NoSuchAlgorithmException {
		HashMap<String, AddressPair> serverAddresses = getServerAddresses();
		List<String> crashedServers = new ArrayList<String>();
		for (Map.Entry<String, AddressPair> entry : serverAddresses.entrySet()) {
			if (!checkConnection(entry.getValue())) {
				System.out.println(entry.getKey() + " is not responding!");
				crashedServers.add(entry.getKey());
			}
		}
		return crashedServers;
	}
	
	public boolean checkConnection(AddressPair addressPair) throws NoSuchAlgorithmException {
		KVStore client = new KVStore(addressPair.getHost(), addressPair.getPort());
    	try {
    		client.connect();
    		client.get("test", addressPair.getHost(), addressPair.getPort());
    	} catch (IOException e) {
    		return false;
    	}
    	return true;
	}
	
	private HashMap<String, AddressPair> getServerAddresses() throws IOException, KeeperException, InterruptedException {
		HashMap<String, AddressPair> serverAddresses = new HashMap<String, AddressPair>();
		String zkPath = "/";
		String connection = zkHostname + ":" + Integer.toString(zkPort) + zkPath;
        ZooKeeper zk = new ZooKeeper(connection, 3000, this);
		List<String> zNodes = zk.getChildren(zkPath, false);
		zk.close();
    	for (String zNode: zNodes) {
    		if (!zNode.equals("zookeeper") && !zNode.equals("fd")) {
    			System.out.println("znode: " + zNode);
    			ZooKeeper rootZk = new ZooKeeper(connection, 3000, null);
    			String data = new String(rootZk.getData(zkPath + zNode, false, null));
    			rootZk.close();
                JSONObject jsonMessage = decodeJsonStr(data);
                System.out.println(data);
                String serverState = (String)jsonMessage.get("State");
                if (serverState.equals("START")) {
	                String serverName = (String)jsonMessage.get("NodeName");
	                String serverHost = (String)jsonMessage.get("NodeHost");
	                int serverPort = Integer.parseInt(jsonMessage.get("NodePort").toString());
	                serverAddresses.put(serverName, new AddressPair(serverHost, serverPort));
                }
    		}
    	}
    	return serverAddresses;
	}
	
	private boolean notifyZookeeper(List<String> failedServers) throws IOException, KeeperException, InterruptedException {
		if (failedServers.size() == 0) {
			return true;
		}
		String connection = zkHostname + ":" + Integer.toString(zkPort) + "/";
        ZooKeeper zk = new ZooKeeper(connection, 3000, this);
		String zkPath = "/fd";
		String data = new String(zk.getData(zkPath, false, null));
		JSONObject jsonMessage = decodeJsonStr(data);
		JSONArray prevFailedServers = (JSONArray) jsonMessage.get("failed");
		List<String> prevFailedList = new ArrayList<String>();
		for (int i = 0; i < prevFailedServers.size(); i++) {
    		String serverName = (String) prevFailedServers.get(i);
    		prevFailedList.add(serverName);
    	}
		List<String> unionFailed = union(failedServers, prevFailedList);
		JSONObject failedServerJson = serializeFailedServers(unionFailed);
		System.out.println(failedServerJson.toString());
		byte[] zkData = failedServerJson.toString().getBytes();
		zk.setData(zkPath, zkData, -1);
		zk.close();
		return true;
	}
	
	public List<String> union(List<String> list1, List<String> list2) {
        Set<String> set = new HashSet<String>();

        set.addAll(list1);
        set.addAll(list2);

        return new ArrayList<String>(set);
    }
	
	@SuppressWarnings("unchecked")
	public JSONObject serializeFailedServers(List<String> failedServers) {
		JSONObject jsonMessage = new JSONObject();
		JSONArray serverArray = new JSONArray();
		for (String serverName : failedServers) {
			serverArray.add(serverName);
		}
		jsonMessage.put("failed", serverArray);
		return jsonMessage;
	}
	
	private JSONObject decodeJsonStr(String data) {
    	JSONObject jsonMessage = null;
        JSONParser parser = new JSONParser();
        try {
            jsonMessage = (JSONObject) parser.parse(data);
        } catch (ParseException e) {
            System.out.println("Error! Unable to parse incoming bytes to json.");
        }
        return jsonMessage;
    }
	
	public static void main(String[] args) {
		if (args.length != 3) {
			System.out.println("Wrong number of arguments passed to failure detector!");
		}
		int intervalSeconds = Integer.parseInt(args[0]);
		String zkHostname = args[1];
		int zkPort = Integer.parseInt(args[2]);
		FailureDetector failureDetector = new FailureDetector(intervalSeconds, zkHostname, zkPort);
		try {
			failureDetector.detect();
		} catch (Exception e) {
			System.out.println("Error! Failure Detector encountered exception!");
			e.printStackTrace();
            System.exit(1);
		}
	}

	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		
	}
}
