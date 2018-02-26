package ecs;

public class ServerConfig {

    public String name;
    public String ip;
    public int port;

    public ServerConfig(String serverName,
                        String serverIP,
                        int    serverPort) {
        name = serverName;
        ip = serverIP;
        port = serverPort;
    }
}