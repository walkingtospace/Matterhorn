package ecs;

// Java Import
public class ECSNode implements IECSNode {

    public String nodeName;
    public String nodeHost;
    public int nodePort;
    public String nameHash;
    public String leftHash;
    public String rightHash;
    public String cacheStrategy;
    public int cacheSize;
    public String state;
    public String target;
    public String transfer;
    public String M1;
    public String M2;
    public String R1;
    public String R2;
    public int numKey;

    public ECSNode(String NodeName,
                   String NodeHost,
                   int NodePort,
                   String nameHash,
                   String LeftHash,
                   String RightHash,
                   String cacheStrategy,
                   int cacheSize,
                   String state,
                   String target,
                   String transfer,
                   String M1,
                   String M2,
                   String R1,
                   String R2,
                   int numKey) {
        this.nodeName = NodeName;
        this.nodeHost = NodeHost;
        this.nodePort = NodePort;
        this.leftHash = LeftHash;
        this.rightHash = RightHash;
        this.nameHash = nameHash;
        this.cacheStrategy = cacheStrategy;
        this.cacheSize = cacheSize;
        this.state = state;
        this.target = target;
        this.transfer = transfer;
        this.M1 = M1;
        this.M2 = M2;
        this.R1 = R1;
        this.R2 = R2;
        this.numKey = numKey;
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
    	res = res + "|nameHash:" + this.nameHash + "|leftHash:" + this.leftHash;
    	res = res + "|rightHash:" + this.rightHash + "|cacheStrategy:" + this.cacheStrategy + "|cacheSize:" + this.cacheSize;
    	res = res + "|state:" + this.state + "|target:" + this.target + "|transfer:" + this.transfer;
    	return res;
    }
}