package client;

import java.util.Objects;

public class AddressKey {
    private final String address;
    private final int port;

    public AddressKey(String address, int port) {
        this.address = address;
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AddressKey)) return false;
        AddressKey otherKey = (AddressKey) o;
        return address == otherKey.address && port == otherKey.port;
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, port);
    }
}
