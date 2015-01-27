package dfs;

import cloudos.kernel.NodeAddress;
import cloudos.kernel.NodeCommunicator;

class FileWriterDescriptor extends FileDescriptor{
	private String fileName;
	private int nBlocks;
	private int currentBlockSize;
	private int blockSize;
	private int nReplicas;

	FileWriterDescriptor(String fileName){
		super(FileMode.WRITER);
		this.fileName = fileName; 
		this.nBlocks = 0;
		this.currentBlockSize = 0;
		this.blockSize = 32*1024*1024; //TODO get from the DFS config file
		this.nReplicas = 3;
	}

	public String getFileName(){
		return this.fileName;
	}

	void setFileName(String fileName){
		this.fileName = fileName;
	}

	int getNumBlocks(){
		return this.nBlocks;
	}

	void setNumBlocks(int nBlocks){
		this.nBlocks = nBlocks;
	}

	void setCurrentBlockSize(int currentBlockSize){
		this.currentBlockSize = currentBlockSize;
	}

	int getCurrentBlockSize(){
		return this.currentBlockSize;
	}

	int getBlockSize(){
		return this.blockSize;
	}
}
