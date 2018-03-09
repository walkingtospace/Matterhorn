package ecs;

import java.math.BigInteger;

public class HashRingEntry {

    public IECSNode escn;
    public String hashValue;

    public HashRingEntry(IECSNode escn, String hashValue) {
        this.escn = escn;
        this.hashValue = hashValue;
    }
    
    @Override
    public String toString() {
    	String res;
    	res = "the node is:" + this.escn;
    	res = res + "\n" + "HashValue:" + this.hashValue;
    	return res;
    	
    }
}