package app_kvServer;

public class Metadata {
	
	public String hostAddress;
	public int port;
	public String hashStart;
	public String hashEnd;
	
	public Metadata (String hostAddress, int port, String hashStart, String hashEnd) {
		
		this.hostAddress = hostAddress;
		this.port = port;
		this.hashStart = hashStart;
		this.hashEnd = hashEnd;
	}
}
