package testing;

import ecs.ECS;
import ecs.HashRingEntry;
import ecs.ECSNode;
import ecs.IECSNode;


import java.util.ArrayList;

import junit.framework.TestCase;
import org.junit.Assert;

public class ECSTest extends TestCase {

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

	public void testAddNode() {
		this.ecs = new ECS(this.configPath);
		this.ecs.addNode("FIFO", 1024);
		Assert.assertEquals(ecs.hashRing.size(), 1);
		HashRingEntry h = ecs.hashRing.get(0);
		Assert.assertEquals(((ECSNode)h.escn).leftHash, "0");
		Assert.assertEquals(((ECSNode)h.escn).rightHash, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
		Assert.assertEquals(((ECSNode)h.escn).cacheStrategy, "FIFO");
		Assert.assertEquals(((ECSNode)h.escn).cacheSize, 1024);
		this.ecs.removeZnode(h.escn);
	}
	
	public void testAddNodes() {
		this.ecs = new ECS(this.configPath);
		this.ecs.addNodes(3, "FIFO", 1024);
		Assert.assertEquals(ecs.hashRing.size(), 3);
		ArrayList<IECSNode> names = new ArrayList<IECSNode>();
		names.add(this.ecs.hashRing.get(0).escn);
		names.add(this.ecs.hashRing.get(1).escn);
		names.add(this.ecs.hashRing.get(2).escn);
		this.ecs.removeZnode(names.get(0));
		this.ecs.removeZnode(names.get(1));
		this.ecs.removeZnode(names.get(2));
	}
	
	public void testStart() {
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
	
	public void testStop() {
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

	public void testShutdown() {
		this.ecs = new ECS(this.configPath);
		this.ecs.addNodes(3,"FIFO", 1024);
		Assert.assertEquals(ecs.hashRing.size(), 3);
		ArrayList<IECSNode> names = new ArrayList<IECSNode>();
		names.add(this.ecs.hashRing.get(0).escn);
		names.add(this.ecs.hashRing.get(1).escn);
		names.add(this.ecs.hashRing.get(2).escn);
		this.ecs.shutdown();
		Assert.assertEquals(ecs.availServers.size(), ecs.availServers.size() + ecs.usedServers.size());
	}
}