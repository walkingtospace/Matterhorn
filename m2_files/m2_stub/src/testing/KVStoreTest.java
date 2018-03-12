package testing;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import client.KVStore;
import common.message.MetaDataEntry;
import junit.framework.TestCase;

public class KVStoreTest extends TestCase {
	
	private KVStore kvStore;
	
	@Test
	public void testInitializeMetadata() throws NoSuchAlgorithmException, IOException {
		kvStore = new KVStore("localhost", 50000);
		TreeMap<String, MetaDataEntry> metadata = kvStore.initializeMetadata("localhost", 50000);
		Assert.assertEquals(metadata.size(), 1);
		String rightHash = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";
		MetaDataEntry initialMetadataEntry = metadata.get(rightHash);
		Assert.assertNotNull(initialMetadataEntry);
		Assert.assertEquals(initialMetadataEntry.leftHash, "0");
		Assert.assertEquals(initialMetadataEntry.rightHash, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
		Assert.assertEquals(initialMetadataEntry.serverHost, "localhost");
		Assert.assertEquals(initialMetadataEntry.serverPort, 50000);
	}
	
	@Test
	public void testBuildTreeMap() throws NoSuchAlgorithmException, IOException {
		kvStore = new KVStore("localhost", 50000);
		List<MetaDataEntry> metadataList = new ArrayList<MetaDataEntry>();
		metadataList.add(new MetaDataEntry("test1", "localhost", 50001, "0", "3FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"));
		metadataList.add(new MetaDataEntry("test2", "localhost", 50002, "40000000000000000000000000000000", "6FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"));
		metadataList.add(new MetaDataEntry("test2", "localhost", 50003, "70000000000000000000000000000000", "9FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"));
		metadataList.add(new MetaDataEntry("test2", "localhost", 50004, "A0000000000000000000000000000000", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"));
		TreeMap<String, MetaDataEntry> metadata = kvStore.buildTreeMap(metadataList);
		Assert.assertEquals(4, metadata.size());
		Assert.assertEquals(50002, metadata.ceilingEntry("400000000000FF000000000000000000").getValue().serverPort);
	}
}
