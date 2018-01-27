package testing;

import org.junit.Test;

import app_kvServer.KVServer;

import client.KVStore;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

import junit.framework.TestCase;

public class AdditionalTest extends TestCase {
	
	private KVServer kvServer;
	
	@Test
	public void testClearStorage() {
		kvServer = new KVServer(1234, 1234, "LRU");
		Exception ex = null;
		String response = null;
		try {
			kvServer.putKV("test", "test");
			kvServer.clearStorage();
			response = kvServer.getKV("test");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response == null);
	}
	
	@Test
	public void testClearStorage() {
		kvServer = new KVServer(1234, 1234, "LRU");
		Exception ex = null;
		String response = null;
		try {
			kvServer.putKV("test", "test");
			kvServer.clearStorage();
			response = kvServer.getKV("test");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response == null);
	}
}
