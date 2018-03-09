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
    	if (h1.length() != h2.length()) {
    		if (h1.length() < h2.length()) {
    			return -1;
    		}
    		return 1;
    	} else {
    		int res = 0;
    		int i = 0;
    		String h1i;
    		String h2i;
    		while(i < h1.length()) {
    			h1i = Character.toString(h1.charAt(i));
    			h2i = Character.toString(h2.charAt(i));
    			if(Integer.parseInt(h1i, 16) < Integer.parseInt(h2i, 16)) {
    				return -1;
    			}
    			if(Integer.parseInt(h1i, 16) > Integer.parseInt(h2i, 16)) {
    				return 1;
    			}
    			i++;
    		}
    		return 0;
    	}
    }
}
