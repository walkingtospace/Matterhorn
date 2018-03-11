package testing;

import client.KVStore;
import junit.framework.TestCase;

import org.junit.Test;
import org.junit.Assert;

public class KVStoreTest extends TestCase {
	
	private KVStore kvClient;
	
	public void setUp() {
		try {
			kvClient = new KVStore("localhost", 50000);
			kvClient.connect();
		} catch (Exception e) {
			System.out.println(e.toString());
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}
	
	@Test
	public void testIsConnected() {
		Assert.assertEquals(true, kvClient.isConnected());
	}
}
