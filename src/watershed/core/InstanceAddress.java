package watershed.core;

import cloudos.kernel.NodeAddress;

public class InstanceAddress {
	private NodeAddress addr;
	private int instance;

	public InstanceAddress(NodeAddress addr, int instance){
		this.addr = addr;
		this.instance = instance;
	}

	public NodeAddress getNodeAddress(){
		return this.addr;
	}

	public void setNodeAddress(NodeAddress addr){
		this.addr = addr;
	}

	public int getInstance(){
		return this.instance;
	}

	public void setInstance(int instance){
		this.instance = instance;
	}
}
