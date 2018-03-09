package ecs;

// Java Import
import java.math.BigInteger;

public class ECSNode implements IECSNode {

    public String nodeName;
    public String nodeHost;
    public int nodePort;
    public String nameHash;
    public String leftHash;
    public String rightHash;

    public ECSNode(String NodeName,
                   String NodeHost,
                   int NodePort,
                   String nameHash,
                   String LeftHash,
                   String RightHash) {
        this.nodeName = NodeName;
        this.nodeHost = NodeHost;
        this.nodePort = NodePort;
        this.leftHash = LeftHash;
        this.rightHash = RightHash;
        this.nameHash = nameHash;
    }

    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    @Override
    public String getNodeName() {
        return this.nodeName;
    }


    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    @Override
    public String getNodeHost() {
        return this.nodeHost;
    }


    /**
     * @return  the port number of the node (ie 8080)
     */
    @Override
    public int getNodePort() {
        return this.nodePort;
    }


    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    @Override
    public String[] getNodeHashRange() {
        String[] result = new String[2];
        result[0] = this.leftHash;
        result[1] = this.rightHash;
        return result;
    }
    
    @Override
    public String toString() {
    	String res = "Name:" + this.nodeName + "|Host:" + this.nodeHost + "|Port" + Integer.toString(this.nodePort);
    	res = res + "|nameHash" + this.nameHash + "|leftHash" + this.leftHash;
    	res = res + "|rightHash" + this.rightHash;
    	return res;
    }
}