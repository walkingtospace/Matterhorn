package testing;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.junit.Test;

import client.AddressPair;
import junit.framework.Assert;
import junit.framework.TestCase;
import failure_detector.FailureDetector;

public class FailureDetectorTest extends TestCase {
	private FailureDetector failureDetector;
	
	@Test
	public void testUnion() {
		int intervalSeconds = 10;
		String zkHostname = "0.0.0.0";
		int zkPort = 3200;
		failureDetector = new FailureDetector(intervalSeconds, zkHostname, zkPort);
		List<String> list1 = new ArrayList<String>();
		list1.add("one");
		list1.add("two");
		List<String> list2 = new ArrayList<String>();
		list2.add("one");
		list2.add("three");
		List<String> unionList = failureDetector.union(list1, list2);
		Assert.assertTrue(unionList.contains("one") && unionList.contains("two") && unionList.contains("three"));
	}
	
	@Test
	public void testCheckConnection() throws NoSuchAlgorithmException {
		int intervalSeconds = 10;
		String zkHostname = "0.0.0.0";
		int zkPort = 3200;
		failureDetector = new FailureDetector(intervalSeconds, zkHostname, zkPort);
		AddressPair addressPair = new AddressPair("0.0.0.0", 9999);
		Assert.assertFalse(failureDetector.checkConnection(addressPair));
	}
	
	@Test
	public void testSerializeFailedServers() {
		int intervalSeconds = 10;
		String zkHostname = "0.0.0.0";
		int zkPort = 3200;
		failureDetector = new FailureDetector(intervalSeconds, zkHostname, zkPort);
		List<String> failedServers = new ArrayList<String>();
		failedServers.add("server1");
		failedServers.add("server4");
		failedServers.add("server6");
		JSONObject jsonMessage = failureDetector.serializeFailedServers(failedServers);
		JSONArray testServers = (JSONArray) jsonMessage.get("failed");
		List<String> testServerList = new ArrayList<String>();
		for (int i = 0; i < testServers.size(); i++) {
    		String serverName = (String) testServers.get(i);
    		testServerList.add(serverName);
    	}
		Assert.assertTrue(testServerList.contains("server1") && testServerList.contains("server4") && testServerList.contains("server6"));
	}
}
