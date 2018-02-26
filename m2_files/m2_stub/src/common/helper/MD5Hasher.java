package common.helper;

import java.security.MessageDigest;
import java.math.BigInteger;

public class MD5Hasher {

    private MessageDigest md;

    public MD5Hasher () {
        md = MessageDigest.getInstance("MD5");
    }

    public BigInteger hashString(String str) {
        md.reset();
        md.update(str.getBytes());
        BigInteger value = new BigInteger(1, md.digest());
        return value;
    }
}
