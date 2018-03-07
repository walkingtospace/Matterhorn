package esc

import java.math.BigInteger;

public class HashRingEntry {

    public IESCNode escn;
    public BigInteger hashValue;

    public HashRingEntry(IESCNode escn, BigInteger hashValue) {
        this.escn = escn;
        this.hashValue = hashValue;
    }
}