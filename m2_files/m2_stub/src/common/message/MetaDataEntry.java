package common.message;

import java.math.BigInteger;

public class MetaDataEntry {
	
	public String serverHost;
    public int serverPort;
    public BigInteger leftHash;
    public BigInteger rightHash;
    
    public MetaDataEntry(String serverHost, int serverPort, BigInteger leftHash, BigInteger rightHash) {
    	
    	this.serverHost = serverHost;
    	this.serverPort = serverPort;
    	this.leftHash = leftHash;
    	this.rightHash = rightHash;
    }
    
}
