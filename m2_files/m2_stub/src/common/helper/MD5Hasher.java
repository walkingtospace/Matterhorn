package common.helper;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;
import javax.xml.bind.DatatypeConverter;

public class MD5Hasher {

    private MessageDigest md;

    public MD5Hasher () throws NoSuchAlgorithmException {
        md = MessageDigest.getInstance("MD5");
    }

    public String hashString(String str) {
        md.reset();
        md.update(str.getBytes());
        String value = DatatypeConverter.printHexBinary(md.digest()).toUpperCase();
        return value;
    }
    
    public int compareHash(String h1, String h2) {
    	// Return -1 if less, 0 if equal, 1 if bigger
    	return 0;
    }
}
