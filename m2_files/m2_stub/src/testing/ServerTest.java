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
		createDummyZNode("test1", "60001");
		createDummyZNode("test2", "60002");
		kvServer = new KVServer("test1", "0.0.0.0", 3200);
		List<MetaDataEntry> metaData = kvServer.fillMetaData();
		Assert.assertEquals(2, metaData.size());
	}
	
	public void testIsKeyInRange() throws KeeperException, InterruptedException, IOException, NoSuchAlgorithmException {
		createDummyZNode("test1", "60001");
		kvServer = new KVServer("test1", "0.0.0.0", 3200);
		boolean result1 = kvServer.isKeyInRange("A");
		boolean result2 = kvServer.isKeyInRange("15");
		Assert.assertEquals(true, result1 && !result2);
	}
	
	public void testUpdate() throws KeeperException, InterruptedException, IOException, NoSuchAlgorithmException {
		createDummyZNode("test1", "60001");
		kvServer = new KVServer("test1", "0.0.0.0", 3200);
		MetaDataEntry metaDataEntry = new MetaDataEntry("test1", "127.0.0.1", 60002, "0", "FF");
		kvServer.update(metaDataEntry);
		
		Assert.assertEquals(60002, kvServer.getPort());
	}
	
	private void createDummyZNode(String nodeName, String nodePort) throws KeeperException, InterruptedException, IOException {
		String connection = "0.0.0.0:3200";
		ZooKeeper zk = new ZooKeeper(connection, 3000, null);
		
		String zkPath = "/test1";
    	JSONObject jsonMessage = new JSONObject();
        jsonMessage.put("NodeName", nodeName);
        jsonMessage.put("NodeHost", "127.0.0.1");
        jsonMessage.put("NodePort", nodePort);
        jsonMessage.put("CacheStrategy", "FIFO");
        jsonMessage.put("CacheSize", "1024");
        jsonMessage.put("State", "STOP");
        jsonMessage.put("NodeHash", "F");
        jsonMessage.put("LeftHash", "0");
        jsonMessage.put("RightHash", "F");
        jsonMessage.put("Target", "null");
        jsonMessage.put("Transfer", "OFF");
        
        byte[] zkData = jsonMessage.toString().getBytes();
        zk.create(zkPath, zkData, ZooDefs.Ids.OPEN_ACL_UNSAFE,
			      CreateMode.PERSISTENT);
        zk.close();
	}
}
