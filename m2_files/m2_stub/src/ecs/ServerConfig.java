package ecs;

public class ServerConfig {

    private String name;
    private String ip;
    private int port;

    public ServerConfig(String serverName, String serverIP, 
                        int serverPort) {
        name = serverName;
        ip = serverIP;
        port = serverPort;
    }

    public String getName() {
        return this.name;
    }

    public String getIP() {
        return this.ip;
    }

    public int getPort() {
        return this.port;
    }
}