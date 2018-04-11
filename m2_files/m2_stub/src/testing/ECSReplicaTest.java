package testing;

import ecs.ECS;
import ecs.HashRingEntry;
import ecs.ECSNode;
import ecs.IECSNode;


import java.util.ArrayList;

import junit.framework.TestCase;
import org.junit.Assert;

public class ECSReplicaTest extends TestCase {

	public ECS ecs;
	public String configPath = "ecs.config";
	
	public void testConnection() {
		try {
			this.ecs = new ECS(this.configPath);
			Assert.assertEquals("1", "1");
		} catch(Exception e) {
			Assert.assertEquals("1", "0");
		}
	}
	
	public void testReplica() {
		this.ecs = new ECS(this.configPath);
		this.ecs.addNode("FIFO", 1024);
		Assert.assertEquals(ecs.hashRing.size(), 1);
		HashRingEntry h = ecs.hashRing.get(0);
		Assert.assertEquals(((ECSNode)h.escn).leftHash, "0");
		Assert.assertEquals(((ECSNode)h.escn).rightHash, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
		Assert.assertEquals(((ECSNode)h.escn).cacheStrategy, "FIFO");
		Assert.assertEquals(((ECSNode)h.escn).cacheSize, 1024);
		Assert.assertEquals(((ECSNode)h.escn).M1, "null");
		Assert.assertEquals(((ECSNode)h.escn).M2, "null");
		Assert.assertEquals(((ECSNode)h.escn).R1, "null");
		Assert.assertEquals(((ECSNode)h.escn).R2, "null");
		this.ecs.removeZnode(h.escn);
	}
	
	public void removeNodeSetToStop() {
		this.ecs = new ECS(this.configPath);
		this.ecs.addNode("FIFO", 1024);
		Assert.assertEquals(ecs.hashRing.size(), 1);
		HashRingEntry h = ecs.hashRing.get(0);
		Assert.assertEquals(((ECSNode)h.escn).leftHash, "0");
		Assert.assertEquals(((ECSNode)h.escn).rightHash, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
		Assert.assertEquals(((ECSNode)h.escn).cacheStrategy, "FIFO");
		Assert.assertEquals(((ECSNode)h.escn).cacheSize, 1024);
		Assert.assertEquals(((ECSNode)h.escn).M1, "null");
		Assert.assertEquals(((ECSNode)h.escn).M2, "null");
		Assert.assertEquals(((ECSNode)h.escn).R1, "null");
		Assert.assertEquals(((ECSNode)h.escn).R2, "null");
		Assert.assertEquals(((ECSNode)h.escn).state, "STOP");
		this.ecs.removeZnode(h.escn);
	}
	
	public void testStartAfterSetReplica() {
		this.ecs = new ECS(this.configPath);
		this.ecs.addNode("FIFO", 1024);
		Assert.assertEquals(ecs.hashRing.size(), 1);
		HashRingEntry h = ecs.hashRing.get(0);
		Assert.assertEquals(((ECSNode)h.escn).leftHash, "0");
		Assert.assertEquals(((ECSNode)h.escn).rightHash, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
		Assert.assertEquals(((ECSNode)h.escn).cacheStrategy, "FIFO");
		Assert.assertEquals(((ECSNode)h.escn).cacheSize, 1024);
		this.ecs.start();
		Assert.assertEquals(((ECSNode)h.escn).state, "START");
		this.ecs.removeZnode(h.escn);		
	}
	
	public void testStopAfterReplica() {
		this.ecs = new ECS(this.configPath);
		this.ecs.addNode("FIFO", 1024);
		Assert.assertEquals(ecs.hashRing.size(), 1);
		HashRingEntry h = ecs.hashRing.get(0);
		Assert.assertEquals(((ECSNode)h.escn).leftHash, "0");
		Assert.assertEquals(((ECSNode)h.escn).rightHash, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
		Assert.assertEquals(((ECSNode)h.escn).cacheStrategy, "FIFO");
		Assert.assertEquals(((ECSNode)h.escn).cacheSize, 1024);
		this.ecs.start();
		Assert.assertEquals(((ECSNode)h.escn).state, "START");
		this.ecs.stop();
		Assert.assertEquals(((ECSNode)h.escn).state, "STOP");
		this.ecs.removeZnode(h.escn);
	}
}