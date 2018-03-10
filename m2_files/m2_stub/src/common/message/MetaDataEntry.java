package common.message;


public class MetaDataEntry {
	
	public String serverName;
	public String serverHost;
    public int serverPort;
    public String leftHash;
    public String rightHash;
    
    public MetaDataEntry(String serverName, String serverHost, int serverPort, String leftHash, String rightHash) {
    	this.serverName = serverName;
    	this.serverHost = serverHost;
    	this.serverPort = serverPort;
    	this.leftHash = leftHash;
    	this.rightHash = rightHash;
    }
    
}
