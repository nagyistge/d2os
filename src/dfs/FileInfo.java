package dfs;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

class FileInfo {
	private String name;
	private int nBlocks;
	private Map<Integer, Map<String, BlockInfo>> blocks;
	private int blockSize;
	private int nReplicas;

	FileInfo(String name){
		this.name = name;
		this.nBlocks = 0;
		this.blockSize = 32*1024*1024; //TODO get from the DFS config file
		this.nReplicas = 3;
		this.blocks = new HashMap<Integer, Map<String, BlockInfo>>();
	}

	void setName(String name){
		this.name = name;
	}

	String getName(){
		return this.name;
	}

	void setNumBlocks(int nBlocks){
		this.nBlocks = nBlocks;
	}

	int getNumBlocks(){
		return this.nBlocks;
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

	void addBlockInfo(BlockInfo blockInfo){
		if(blockInfo.getBlockId()>this.nBlocks) throw new IllegalArgumentException();
		if(blockInfo.getNodeName()==null) throw new IllegalArgumentException();

		Integer blockId = new Integer(blockInfo.getBlockId());
		if(!this.blocks.keySet().contains(blockId)){
			this.blocks.put(blockId, new HashMap<String, BlockInfo>());
		}
		this.blocks.get(blockId).put(blockInfo.getNodeName(), blockInfo);
	}

	Map<String, BlockInfo> getBlockLocations(int blockId){
		return this.blocks.get(new Integer(blockId));
	}

	BlockInfo getBlockInfo(int blockId){
		Map<String, BlockInfo> locations = this.blocks.get(new Integer(blockId));
		BlockInfo b = null;
		if(locations==null) return null;
		for(String nodeName: locations.keySet()){
			b = locations.get(nodeName);
			break;
		}
		return b;
	}

	void removeBlockInfo(String nodeName, int blockId){
		this.blocks.get(new Integer(blockId)).remove(nodeName);
	}
}
