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

import client.KVStore;

import common.message.KVMessage;
import common.message.MetaDataEntry;
import common.message.KVMessage.StatusType;

import app_kvServer.KVServer;
import junit.framework.TestCase;

public class ServerTest extends TestCase{
	
	private KVServer kvServer;
	private KVStore kvClient;
	
	public void testConnection() throws KeeperException, InterruptedException, IOException{
		
		createDummyZNode("test1", "60001");
		
		kvServer = new KVServer("test1", "0.0.0.0", 3200);
		String state = kvServer.getState();
		if (state.equals("START") || state.equals("STOP")) {
			Assert.assertEquals("1", "1");
		} else {
			Assert.assertEquals("1", "0");
		}
		deleteDummyZNode("test1");
	}
	
	public void testFillUpMetaData() throws KeeperException, InterruptedException, IOException {
		createDummyZNode("test2", "60002");
		createDummyZNode("test3", "60003");
		kvServer = new KVServer("test2", "0.0.0.0", 3200);
		List<MetaDataEntry> metaData = kvServer.fillMetaData();
		Assert.assertEquals(true, metaData.size() != 1);
		deleteDummyZNode("test2");
		deleteDummyZNode("test3");
	}
	
	public void testIsKeyInRange() throws KeeperException, InterruptedException, IOException, NoSuchAlgorithmException {
		createDummyZNode("test4", "60004");
		kvServer = new KVServer("test4", "0.0.0.0", 3200);
		boolean result1 = kvServer.isKeyInRange("A", null, null);
		boolean result2 = kvServer.isKeyInRange("15", null, null);
		System.out.println(result1);
		System.out.println(result2);
		Assert.assertEquals(true, result1 && result2);
		deleteDummyZNode("test4");
	}
	
	public void testUpdate() throws KeeperException, InterruptedException, IOException, NoSuchAlgorithmException {
		createDummyZNode("test5", "60005");
		kvServer = new KVServer("test5", "0.0.0.0", 3200);
		MetaDataEntry metaDataEntry = new MetaDataEntry("test1", "127.0.0.1", 60005, "0", "FF");
		kvServer.update(metaDataEntry);
		
		Assert.assertEquals(60005, kvServer.getPort());
		deleteDummyZNode("test5");

	}
	
	public void testStartAndStop() throws Exception {
		createDummyZNode("test6", "60006");
		kvServer = new KVServer("test6", "0.0.0.0", 3200);
//		kvServer.run();
//		kvClient = new KVStore("127.0.0.1", 60006);
//		kvClient.connect();
//		KVMessage result = kvClient.put("test", "test");
//		StatusType status = result.getStatus();
//		Assert.assertEquals(true, status == StatusType.SERVER_STOPPED);
		String state = kvServer.getState();
//		System.out.println("state: " + state);
		Assert.assertEquals(true, state.equals("STOP"));
		kvServer.start();
//		result = kvClient.put("test", "test");
//		status = result.getStatus();
//		Assert.assertEquals(true, status == StatusType.PUT_SUCCESS);
		state = kvServer.getState();
		Assert.assertEquals(true, state.equals("START"));
		kvServer.stop();
//		result = kvClient.get("test");
//		status = result.getStatus();
//		Assert.assertEquals(true, status == StatusType.SERVER_STOPPED);
//		kvClient.disconnect();
		state = kvServer.getState();
		Assert.assertEquals(true, state.equals("STOP"));
//		kvServer.close();
		deleteDummyZNode("test6");
	}
	
	public void testLockWrite() throws Exception {
		createDummyZNode("test7", "60007");
		kvServer = new KVServer("test7", "0.0.0.0", 3200);
//		kvServer.run();
//		kvClient = new KVStore("127.0.0.1", 60007);
//		kvClient.connect();
		kvServer.start();
		kvServer.lockWrite();
//		KVMessage result = kvClient.put("test", "test");
//		StatusType status = result.getStatus();
//		Assert.assertEquals(true, status == StatusType.SERVER_WRITE_LOCK);
		Assert.assertEquals(true, kvServer.isLocked());
		kvServer.unlockWrite();
//		result = kvClient.put("test", "test");
//		status = result.getStatus();
//		Assert.assertEquals(true, status == StatusType.PUT_SUCCESS);
//		kvClient.disconnect();
		Assert.assertEquals(false, kvServer.isLocked());
//		kvServer.close();
		deleteDummyZNode("test7");
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
        jsonMessage.put("M1", "null");
        jsonMessage.put("M2", "null");
        jsonMessage.put("R1", "null");
        jsonMessage.put("R2", "null");
        
        byte[] zkData = jsonMessage.toString().getBytes();
        zk.create(zkPath, zkData, ZooDefs.Ids.OPEN_ACL_UNSAFE,
			      CreateMode.PERSISTENT);
        zk.close();
	}
	
	private void deleteDummyZNode (String nodeName) throws IOException, InterruptedException {
		String connection = "0.0.0.0:3200";
		ZooKeeper zk = new ZooKeeper(connection, 3000, null);
		String path = "/" + nodeName;
    	try {
			zk.delete(path,zk.exists(path,true).getVersion());
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
    	zk.close();
	}
}