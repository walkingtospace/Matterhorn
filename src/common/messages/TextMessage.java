package common.messages;

import java.io.Serializable;

import java.io.StringWriter;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import common.messages.KVMessage.StatusType;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Represents a simple text message, which is intended to be received and sent 
 * by the server.
 */
public class TextMessage implements Serializable, KVMessage{

	private static final long serialVersionUID = 5549512212003782618L;
	JSONObject jsonMessage = null;
	private String msg = null;
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
//		StringWriter out = new StringWriter();
//		jsonMessage.writeJSONString(out);
	}
	
	public TextMessage(StatusType status, String key, String value) {
		jsonMessage = new JSONObject();
		this.jsonMessage.put("status", status.toString());
		this.jsonMessage.put("key", key);
		this.jsonMessage.put("value", value);
		this.msg = jsonMessage.toString();
		isClient = false;
//		StringWriter out = new StringWriter();
//		jsonMessage.writeJSONString(out);
	}

	/**
     * Constructs a TextMessage object with a given array of bytes that 
     * forms the message. Used for received message
     * 
     * @param bytes the bytes that form the message in ASCII coding.
     */
	public TextMessage(byte[] bytes) {
		this.msg = new String(toByteArray(bytes.toString()));
		JSONParser parser = new JSONParser();
		try {
			jsonMessage = (JSONObject) parser.parse(this.msg);
		} catch (ParseException e) {
			logger.error("Error! " +
        			"Unable to parser incoming bytes to json. \n", e);
		}
		isClient = jsonMessage.containsKey("operation") ? true : false;
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
	 * 		in ASCII coding.
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
		return isClient ? (StatusType) jsonMessage.get("operation") : (StatusType) jsonMessage.get("status");
	}
	
}
