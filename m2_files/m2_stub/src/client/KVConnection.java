package client;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class KVConnection {
	private Socket socket;
	private OutputStream output;
    private InputStream input;
    
    public KVConnection(Socket socket, InputStream input, OutputStream output) {
    	this.socket = socket;
    	this.input = input;
    	this.output = output;
    }
}
