package client;

import java.util.Objects;

public class AddressPair {
    private final String address;
    private final int port;

    public AddressPair(String address, int port) {
        this.address = address;
        this.port = port;
    }
    
    public String getHost() {
    	return this.address;
    }
    
    public int getPort() {
    	return this.port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AddressPair)) return false;
        AddressPair otherKey = (AddressPair) o;
        return address == otherKey.address && port == otherKey.port;
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, port);
    }
}
