package dfs;

class BlockInfo {
	private String fileName;
	private String nodeName;
	private String path;
	private int blockId;
	private int blockSize;
	private int nReplicas;

	//public BlockInfo(){}

	BlockInfo(String fileName, int blockSize, int nReplicas, int blockId, String nodeName, String path){
		this.fileName = fileName;
		this.blockId = blockId;
		this.nodeName = nodeName;
		this.path = path;
		this.blockSize = blockSize;
		this.nReplicas = nReplicas;
	}

	void setFileName(String fileName){
		this.fileName = fileName;
	}

	String getFileName(){
		return this.fileName;
	}

	void setBlockSize(int blockSize){
		this.blockSize = blockSize;
	}

	int getBlockSize(){
		return this.blockSize;
	}

	void setNumReplicas(int nReplicas){
		this.nReplicas = nReplicas;
	}

	int getNumReplicas(){
		return this.nReplicas;
	}

	void setBlockId(int blockId){
		this.blockId = blockId;
	}

	int getBlockId(){
		return this.blockId;
	}

	void setNodeName(String nodeName){
		this.nodeName = nodeName;
	}

	String getNodeName(){
		return this.nodeName;
	}

	void setPath(String path){
		this.nodeName = nodeName;
	}

	String getPath(){
		return this.path;
	}
}
