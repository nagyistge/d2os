package d2os.kernel;

import d2os.kernel.info.NodeInfo;

public class NodeAddress {
	private final NodeInfo nodeInfo;
	private final int port;

	public NodeAddress(NodeInfo nodeInfo, int port){
		this.nodeInfo = nodeInfo;
		this.port = port;
	}

	public NodeInfo getNodeInfo(){
		return this.nodeInfo;
	}
	/*
	public void setNodeInfo(NodeInfo nodeInfo){
		this.nodeInfo = nodeInfo;
	}
	*/
	public int getPort(){
		return this.port;
	}
	/*
	public void setPort(int port){
		this.port = port;
	}
	*/
}
