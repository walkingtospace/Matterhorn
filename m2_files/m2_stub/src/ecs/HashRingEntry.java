package ecs;

import java.math.BigInteger;

public class HashRingEntry {

    public IECSNode escn;
    public String hashValue;

    public HashRingEntry(IECSNode escn, String hashValue) {
        this.escn = escn;
        this.hashValue = hashValue;
    }
}