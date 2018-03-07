package ecs;

import java.math.BigInteger;

public class HashRingEntry {

    public IECSNode escn;
    public BigInteger hashValue;

    public HashRingEntry(IECSNode escn, BigInteger hashValue) {
        this.escn = escn;
        this.hashValue = hashValue;
    }
}