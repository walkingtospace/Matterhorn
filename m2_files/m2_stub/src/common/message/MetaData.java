package common.message;

import java.util.HashMap;
import java.util.Map;
import java.lang.Exception;
import java.math.BigInteger;

public class MetaData {

    private Map<String, MetaDataEntry> data;

    public MetaData() {
        data = new HashMap<String, MetaDataEntry>();
    }

    public boolean addEntry(String serverName,
                    String serverHost,
                    int serverPort,
                    BigInteger leftHash,
                    BigInteger rightHash) {
        try {
            MetaDataEntry entry = new MetaDataEntry(serverHost,
                                                    serverPort,
                                                    leftHash,
                                                    rightHash);
            data.put(serverName, entry);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean deleteEntry(String serverName) {
        try {
            data.remove(serverName);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public Map<String, MetaDataEntry> getMetaData() {
        return data;
    }
}