package ecs;

// Java Import
import java.math.BigInteger;

public class ECSNode implements IECSNode {

    private String nodeName;
    private String nodeHost;
    private int nodePort;
    private BigInteger leftHash;
    private BigInteger rightHash;

    public ECSNode(String iNodeName, String iNodeHost, int iNodePort, BigInteger iLeftHash, BigInteger iRightHash) {
        nodeName = iNodeName;
        nodeHost = iNodeHost;
        nodePort = iNodePort;
        leftHash = iLeftHash;
        rightHash = iRightHash;
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
        result[0] = this.leftHash.toString(16);
        result[1] = this.rightHash.toString(16);
        return result;
    }


    public BigInteger getLeftHash() {
        return this.leftHash;
    }

    public BigInteger getRightHash() {
        return this.rightHash;
    }
}