package dfs;

import cloudos.kernel.NodeAddress;
import cloudos.kernel.NodeCommunicator;

class FileReaderDescriptor extends FileDescriptor{
	private FileInfo fileInfo;
	private int currentBlockId;

	FileReaderDescriptor(FileInfo fileInfo){
		super(FileMode.READER);
		this.fileInfo = fileInfo; 
		this.currentBlockId = 0;
	}

	public String getFileName(){
		return this.fileInfo.getName();
	}

	FileInfo getFileInfo(){
		return this.fileInfo;
	}

	void setFileInfo(FileInfo fileInfo){
		this.fileInfo = fileInfo;
	}

	void setCurrentBlockId(int currentBlockId){
		this.currentBlockId = currentBlockId;
	}

	int getCurrentBlockId(){
		return this.currentBlockId;
	}
}

