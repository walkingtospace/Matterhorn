package common.message;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import ecs.IECSNode;

/**
 * Represents a simple text message, which is intended to be received and sent 
 * by the server.
 */
public class TextMessage implements Serializable, KVMessage{

    private static final long serialVersionUID = 5549512212003782618L;
    JSONObject jsonMessage = null;
    private String msg = null;
    private byte[] msgBytes;
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;
    private boolean isClient;
    private static Logger logger = Logger.getRootLogger();


    public TextMessage(String opt, String key, String value) {
        jsonMessage = new JSONObject();
        this.jsonMessage.put("operation", opt);
        this.jsonMessage.put("key", key);
        this.jsonMessage.put("value", value);
        this.msg = jsonMessage.toString();
        isClient = true;
    }
    
    
    public TextMessage(StatusType status, String key, String value) {
        jsonMessage = new JSONObject();
        this.jsonMessage.put("status", status.toString());
        this.jsonMessage.put("key", key);
        this.jsonMessage.put("value", value);
        this.msg = jsonMessage.toString();
        isClient = false;
    }

    public TextMessage(StatusType status, String key, String value, List<MetaDataEntry> metaData) {
        jsonMessage = new JSONObject();
        this.jsonMessage.put("status", status.toString());
        this.jsonMessage.put("key", key);
        this.jsonMessage.put("value", value);
        JSONArray metaDataArray = new JSONArray();
        for (MetaDataEntry entry : metaData) {
        	JSONObject metaDataObject = new JSONObject();
        	metaDataObject.put("serverName", entry.serverName);
        	metaDataObject.put("serverHost", entry.serverHost);
        	metaDataObject.put("serverPort", entry.serverPort);
        	metaDataObject.put("leftHash", entry.leftHash);
        	metaDataObject.put("rightHash", entry.rightHash);
        	metaDataArray.add(metaDataObject);
        }
        this.jsonMessage.put("metadata", metaDataArray);
        this.msg = jsonMessage.toString();
        isClient = false;
//      StringWriter out = new StringWriter();
//      jsonMessage.writeJSONString(out);
    }


    /**
     * Constructs a TextMessage object with a given array of bytes that 
     * forms the message. Used for received message
     * 
     * @param bytes the bytes that form the message in ASCII coding.
     */
    public TextMessage(byte[] bytes) {
        this.msgBytes = addCtrChars(bytes);
        this.msg = new String(msgBytes);
//      this.msg = new String(toByteArray(bytes.toString()));
        JSONParser parser = new JSONParser();
        try {
            jsonMessage = (JSONObject) parser.parse(this.msg);
            isClient = jsonMessage.containsKey("operation") ? true : false;
        } catch (ParseException e) {
            logger.error("Error! " +
                    "Unable to parse incoming bytes to json. \n", e);
        }
    }


    /**
     * Constructs a TextMessage object with a given String that
     * forms the message. Only used for establishing connection
     * 
     * @param msg the String that forms the message.
     */
    public TextMessage(String msg) {
        this.msg = msg;
    }


    /**
     * Returns the content of this TextMessage as a String.
     * 
     * @return the content of this message in String format.
     */
    public String getMsg() {
        return msg;
    }


    /**
     * Returns an array of bytes that represent the ASCII coded message content.
     * 
     * @return the content of this message as an array of bytes 
     *      in ASCII coding.
     */
    public byte[] getMsgBytes() {
        return toByteArray(msg);
    }


    private byte[] addCtrChars(byte[] bytes) {
        byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
        byte[] tmp = new byte[bytes.length + ctrBytes.length];
        
        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
        
        return tmp;     
    }


    private byte[] toByteArray(String s){
        byte[] bytes = s.getBytes();
        byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
        byte[] tmp = new byte[bytes.length + ctrBytes.length];
        
        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
        
        return tmp;     
    }


    @Override
    public String getKey() {
        if (jsonMessage == null)
            return null;
        return (String) jsonMessage.get("key");
    }


    @Override
    public String getValue() {
        if (jsonMessage == null)
            return null;
        return (String) jsonMessage.get("value");
    }


    @Override
    public StatusType getStatus() {
        if (jsonMessage == null)
            return null;
        return StatusType.valueOf((String) (isClient ? jsonMessage.get("operation") : jsonMessage.get("status")));
    }
    
    @SuppressWarnings("unchecked")
	public List<MetaDataEntry> getMetaData() {
    	if (jsonMessage == null || jsonMessage.get("metadata") == null)
            return null;
    	List<MetaDataEntry> metaDataList = new ArrayList<MetaDataEntry>();
    	JSONArray metaDataArray = (JSONArray) jsonMessage.get("metadata");
    	for (int i = 0; i < metaDataArray.size(); i++) {
    		JSONObject obj = (JSONObject) metaDataArray.get(i);
    		MetaDataEntry metaDataEntry = new MetaDataEntry((String) obj.get("serverName"),
    				(String) obj.get("serverHost"), (int) obj.get("serverPort"),
    				(String) obj.get("leftHash"), (String) obj.get("rightHash"));
    		metaDataList.add(metaDataEntry);
    	}
    	return metaDataList;
    }

	@Override
	public IECSNode getResponsibleServer() {
		// TODO Auto-generated method stub
		return null;
	}
}
