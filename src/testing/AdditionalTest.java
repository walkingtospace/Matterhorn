package testing;

import org.junit.Test;

import app_kvServer.KVServer;
import app_kvClient.KVClient;

import client.KVStore;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

import junit.framework.TestCase;

/**
 * This class contains a set of JUnit tests for a key-value storage system,
 * including tests for clearing, checking for existence, and deleting key-value
 * pairs, as well as a test for the user interface and for handling spaces in
 * keys.
 */
public class AdditionalTest extends TestCase {
	
	private KVServer kvServer;
	private KVStore kvClient;

	/**
	 * This method sets up the server and client objects for testing.
	 */
	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	/**
	 * This method disconnects the client from the server.
	 */
	public void tearDown() {
		kvClient.disconnect();
	}

	/**
	 * This test checks if the clearStorage() method works correctly.
	 */
	@Test
	public void testClearStorage() {
		kvServer = new KVServer(1234, 1234, "LRU");
		Exception ex = null;
		boolean response1 = true;
		boolean response2 = true;
		try {
			kvServer.putKV("test", "test");
			kvServer.putKV("test1", "test1");
			kvServer.clearStorage();
			response1 = kvServer.inStorage("test");
			response2 = kvServer.inStorage("test1");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response1 == false && response2 == false);
	}
	
	/**
	 * This test checks if the inStorage() method works correctly.
	 */
	@Test
	public void testInStorage() {
		kvServer = new KVServer(1234, 1234, "LRU");
		Exception ex = null;
		boolean response1 = false;
		boolean response2 = true;
		try {
			kvServer.putKV("test", "test");
			response1 = kvServer.inStorage("test");
			response2 = kvServer.inStorage("gg");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response1 == true && response2 == false);
	}
	
	/**
	 * This test checks if the deleteKV() method works correctly.
	 */
	@Test
	public void testDeleteKV() {
		kvServer = new KVServer(1234, 1234, "LRU");
		Exception ex = null;
		boolean response = true;
		try {
			kvServer.putKV("test", "test");
			kvServer.deleteKV("test");
			response = kvServer.inStorage("test") || kvServer.inCache("test");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response == false);
	}
	
	/**
	 * This test checks if the user interface works correctly.
	 */
	@Test
	public void testShellUI() {
		// Test connect command
		KVClient ui = new KVClient();
		kvServer = new KVServer(50000, 1024, "FIFO");
		ui.handleCommand("connect localhost 50000");
		ui.handleCommand("put testUI testUI"); // Insert
		ui.handleCommand("put testUI"); // Delete
		boolean response = kvServer.inStorage("testUI");
		assertTrue(response == false);
	}
	
	/**
	 * This test checks if the put() method works correctly with keys that contain
	 * spaces.
	 */
	@Test
	public void testPutWithSpace() {
		String key = "space";
		String value = "my string";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, "");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
		
		try {
			response = kvClient.put(key, "");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}
}
