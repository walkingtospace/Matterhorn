package common.helper;

import java.util.Comparator;

public class HashComparator implements Comparator<String> {

	@Override
	public int compare(String h1, String h2) {
		// Return -1 if less, 0 if equal, 1 if bigger
    	if (h1.length() != h2.length()) {
    		if (h1.length() < h2.length()) {
    			return -1;
    		}
    		return 1;
    	} else {
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
