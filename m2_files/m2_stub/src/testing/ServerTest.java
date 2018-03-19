package testing;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.json.simple.JSONObject;
import org.junit.Assert;

import common.message.MetaDataEntry;

import app_kvServer.KVServer;
import junit.framework.TestCase;

public class ServerTest extends TestCase{
	
	public KVServer kvServer;
	
	public void testConnection() throws KeeperException, InterruptedException, IOException{
		
		createDummyZNode("test1", "60001");
		
		kvServer = new KVServer("test1", "0.0.0.0", 3200);
		String state = kvServer.getState();
		if (state.equals("START") || state.equals("STOP")) {
			Assert.assertEquals("1", "1");
		} else {
			Assert.assertEquals("1", "0");
		}
	}
	
	public void testFillUpMetaData() throws KeeperException, InterruptedException, IOException {
		createDummyZNode("test2", "60002");
		createDummyZNode("test3", "60003");
		kvServer = new KVServer("test2", "0.0.0.0", 3200);
		List<MetaDataEntry> metaData = kvServer.fillMetaData();
		Assert.assertEquals(true, metaData.size() != 1);
	}
	
	public void testIsKeyInRange() throws KeeperException, InterruptedException, IOException, NoSuchAlgorithmException {
		createDummyZNode("test4", "60004");
		kvServer = new KVServer("test4", "0.0.0.0", 3200);
		boolean result1 = kvServer.isKeyInRange("A");
		boolean result2 = kvServer.isKeyInRange("15");
		System.out.println(result1);
		System.out.println(result2);
		Assert.assertEquals(true, result1 && result2);
	}
	
	public void testUpdate() throws KeeperException, InterruptedException, IOException, NoSuchAlgorithmException {
		createDummyZNode("test5", "60005");
		kvServer = new KVServer("test5", "0.0.0.0", 3200);
		MetaDataEntry metaDataEntry = new MetaDataEntry("test1", "127.0.0.1", 60005, "0", "FF");
		kvServer.update(metaDataEntry);
		
		Assert.assertEquals(60005, kvServer.getPort());
	}
	
	private void createDummyZNode(String nodeName, String nodePort) throws KeeperException, InterruptedException, IOException {
		String connection = "0.0.0.0:3200";
		ZooKeeper zk = new ZooKeeper(connection, 3000, null);
		
		String zkPath = "/" + nodeName;
    	JSONObject jsonMessage = new JSONObject();
        jsonMessage.put("NodeName", nodeName);
        jsonMessage.put("NodeHost", "127.0.0.1");
        jsonMessage.put("NodePort", nodePort);
        jsonMessage.put("CacheStrategy", "FIFO");
        jsonMessage.put("CacheSize", "1024");
        jsonMessage.put("State", "STOP");
        jsonMessage.put("NodeHash", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
        jsonMessage.put("LeftHash", "0");
        jsonMessage.put("RightHash", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
        jsonMessage.put("Target", "null");
        jsonMessage.put("Transfer", "OFF");
        
        byte[] zkData = jsonMessage.toString().getBytes();
        zk.create(zkPath, zkData, ZooDefs.Ids.OPEN_ACL_UNSAFE,
			      CreateMode.PERSISTENT);
        zk.close();
	}
}
