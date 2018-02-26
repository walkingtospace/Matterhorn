package common.messages;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class OperationMessage implements Serializable, AdminMessage {

    private static final long serialVersionUID = 5549512212003782618L;
    JSONObject jsonMessage = null;
    private String msg = null;
    private byte[] msgBytes;
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;
    private static Logger logger = Logger.getRootLogger();


    public OperationMessage(OptType opt) {
        jsonMessage = new JSONObject();
        this.jsonMessage.put("operation", opt.toString());
        this.msg = jsonMessage.toString();
    }


    public OperationMessage(OptStatus opt_status) {
        jsonMessage = new JSONObject();
        this.jsonMessage.put("status", opt_status.toString());
        this.msg = jsonMessage.toString();
    }


    public OperationMessage(byte[] bytes) {
        this.msgBytes = addCtrChars(bytes);
        this.msg = new String(msgBytes);
        JSONParser parser = new JSONParser();
        try {
            jsonMessage = (JSONObject) parser.parse(this.msg);
        } catch (ParseException e) {
            logger.error("Error! " +
                    "Unable to parse incoming bytes to json. \n", e);
        }
    }


    public OperationMessage(String msg) {
        this.msg = msg;
    }


    @Override
    public OptType getOptType() {
        if (jsonMessage == null)
            return null;
        return OptType.valueOf((String)(jsonMessage.get("operation")));
    }


    @Override
    public OptStatus getOptStatus() {
        if (jsonMessage == null)
            return null;
        return OptStatus.valueOf((String)(jsonMessage.get("status")));
    }


    public String getMsg() {
        return msg;
    }


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

}