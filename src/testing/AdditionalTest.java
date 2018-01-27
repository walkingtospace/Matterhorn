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
}
