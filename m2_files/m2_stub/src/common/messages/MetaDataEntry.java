package common.messages;

public class MetaDataEntry {
	
	public String serverHost;
    public int serverPort;
    public int leftHash;
    public int rightHash;
    
    public MetaDataEntry(String serverHost, int serverPort, int leftHash, int rightHash) {
    	
    	this.serverHost = serverHost;
    	this.serverPort = serverPort;
    	this.leftHash = leftHash;
    	this.rightHash = rightHash;
    }
}
