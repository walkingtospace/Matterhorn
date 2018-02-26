package common.messages;

import java.util.Map;
import java.lang.Exception;

public class MetaData {

    private Map<String, MetaDataEntry> data;

    public MetaData() {
        data = new Map<String, MetaDataEntry>();
    }

    public boolean addEntry(String serverName,
                    String serverHost,
                    int serverPort,
                    int leftHash,
                    int rightHash) {
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

    public MetaData getMetaData() {
        return data;
    }
}