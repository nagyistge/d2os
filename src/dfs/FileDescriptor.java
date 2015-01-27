package dfs;

import cloudos.kernel.NodeAddress;
import cloudos.kernel.NodeCommunicator;

public abstract class FileDescriptor {
	private FileMode mode;
	private NodeAddress nodeAddr;
	private NodeCommunicator comm;

	FileDescriptor(FileMode mode){
		this.mode = mode;
		this.nodeAddr = null;
		this.comm = null;
	}

	public FileMode getMode(){
		return this.mode;
	}

	public abstract String getFileName();

	void setMode(FileMode mode){
		this.mode = mode;
	}

	void setNodeAddress(NodeAddress addr){
		this.nodeAddr = addr;
	}

	NodeAddress getNodeAddress(){
		return this.nodeAddr;
	}

	void setNodeCommunicator(NodeCommunicator comm){
		this.comm = comm;
	}

	NodeCommunicator getNodeCommunicator(){
		return this.comm;
	}
}

