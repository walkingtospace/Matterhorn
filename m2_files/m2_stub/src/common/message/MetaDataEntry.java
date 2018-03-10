package common.message;

import java.math.BigInteger;

public class MetaDataEntry {
	
	public String serverHost;
    public int serverPort;
    public String leftHash;
    public String rightHash;
    
    public MetaDataEntry(String serverHost, int serverPort, String leftHash, String rightHash) {
    	
    	this.serverHost = serverHost;
    	this.serverPort = serverPort;
    	this.leftHash = leftHash;
    	this.rightHash = rightHash;
    }
    
}
