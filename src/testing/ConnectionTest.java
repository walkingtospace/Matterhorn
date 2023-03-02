package testing;

import java.net.UnknownHostException;

import client.KVStore;

import junit.framework.TestCase;


public class ConnectionTest extends TestCase {

	
	public void testConnectionSuccess() {
		
		Exception ex = null;
		
		KVStore kvClient = new KVStore("localhost", 50000); // Create a new key-value store client object
		try {
			kvClient.connect(); // Attempt to connect to the store
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex); // Assert that no exception was thrown
	}
	
	
	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", 50000); // Create a new key-value store client object with an unknown host
		
		try {
			kvClient.connect(); // Attempt to connect to the store
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof UnknownHostException); // Assert that an UnknownHostException was thrown
	}
	
	
	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 123456789); // Create a new key-value store client object with an illegal port
		
		try {
			kvClient.connect(); // Attempt to connect to the store
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof IllegalArgumentException);
	}
	
	

	
}

