package dfs;

import java.io.File;
import java.io.IOException;

import java.util.Map;
//import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import java.util.Set;
import java.util.TreeSet;

import java.util.AbstractMap.SimpleEntry;

import com.google.gson.reflect.TypeToken;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import cloudos.kernel.ModuleMaster;
import cloudos.kernel.info.NodeInfo;
import cloudos.kernel.SystemCallRequest;
import cloudos.kernel.SystemCallReply;
import cloudos.kernel.SystemCallErrorType;
import cloudos.kernel.NodeAddress;
import cloudos.kernel.Global;
import cloudos.kernel.SystemCallInterface;

import cloudos.scheduler.RandomScheduler;
import cloudos.scheduler.RoundRobinScheduler;

import cloudos.util.Json;
import cloudos.util.Logger;
import cloudos.util.TreeNode;


public class Master extends ModuleMaster{
	private FileNamespace namespace;
	private Map<String, FileInfo> files;
	private RoundRobinScheduler scheduler;

	public void start(){
		super.start();
		this.files = new ConcurrentHashMap<String, FileInfo>();
		this.namespace = new FileNamespace();
		this.scheduler = new RoundRobinScheduler();
	}
	
	public void finish(){
		super.finish();
	}
	
	public void onSlaveDead(String nodeName){
		super.onSlaveDead(nodeName);

		for(String key: this.files.keySet()){
			FileInfo fileInfo = this.files.get(key);
			for(int blockId=0; blockId<fileInfo.getNumBlocks();blockId++){
				if(fileInfo.getBlockLocations(blockId).containsKey(nodeName)){
					fileInfo.removeBlockInfo(nodeName, blockId);
				}
			}
		}

		for(String key: this.files.keySet()){
			FileInfo fileInfo = this.files.get(key);
			for(int blockId=0; blockId<fileInfo.getNumBlocks();blockId++){
				if(fileInfo.getBlockLocations(blockId).size()<fileInfo.getNumReplicas()){
					//TODO replicate this data block
				}
			}
		}
	}
	
	public void addFolder(String path) throws IOException{
		this.namespace.addFolder(path);
	}

	public void addFolders(String path) throws IOException{
		this.namespace.addFolders(path);
	}

	public void addFile(String path) throws IOException{
		File file = new File(path);
		if(!this.files.keySet().contains(file.getPath())){
			this.namespace.addFolders(file.getParent());
			this.namespace.addFile(file.getPath());
			this.files.put(file.getPath(), new FileInfo(file.getPath()));
		}
	}

	private String scheduleNode(){
		Logger.info("in scheduling node");
		Logger.info(Json.dumps(getEnvironment().getSlaves()));
		String nodeName = this.scheduler.nextNodeName(getEnvironment().getSlaves());
		Logger.info("scheduling node:"+nodeName);
		return nodeName;
	}

	private String getPath(String nodeName, String fileName, int blockId){
		String path = getEnvironment().getSlaveInfo(nodeName).getWorkspace();
		path += "modules/dfs/"+Base64.encodeBase64String(StringUtils.getBytesUtf8(fileName))+"/"+blockId+".block";
		return path;
	}

	public BlockInfo appendBlock(String path) throws IOException{
		File file = new File(path);
		FileInfo fileInfo = this.files.get(file.getPath());
		if(fileInfo==null) throw new IOException("File doesn't exist");
		int nBlocks = fileInfo.getNumBlocks();
		Logger.info("adding block "+nBlocks);
		fileInfo.setNumBlocks(nBlocks+1);
		Logger.info("scheduling node");
		String nodeName = scheduleNode();
		if(nodeName==null) throw new IOException("There is no available slave");
		Logger.info("node scheduled");
		String blockPath = getPath(nodeName, file.getPath(), nBlocks);
		Logger.info("creating block");
		BlockInfo blockInfo = new BlockInfo(file.getPath(), fileInfo.getBlockSize(), fileInfo.getNumReplicas(), nBlocks, nodeName, blockPath);
		//fileInfo.addBlockInfo(blockInfo); //delay it for the time the data block has already been stored
		return blockInfo;
	}

	public void updateBlockInfo(BlockInfo blockInfo) throws IOException{
		FileInfo fileInfo = this.files.get(blockInfo.getFileName());
		if(fileInfo==null) throw new IOException("File doesn't exist");
		fileInfo.addBlockInfo(blockInfo);
	}

	public BlockInfo addBlock(String path, int blockId) throws IOException{
		File file = new File(path);
		FileInfo fileInfo = this.files.get(file.getPath());
		if(fileInfo==null) throw new IOException("File doesn't exist");
		if(blockId>=fileInfo.getNumBlocks()) throw new IOException("Invalid block identifier");
		String nodeName = scheduleNode();
		String blockPath = getPath(nodeName, file.getPath(), blockId);
		BlockInfo blockInfo = new BlockInfo(file.getPath(), fileInfo.getBlockSize(), fileInfo.getNumReplicas(), blockId, nodeName, blockPath);
		fileInfo.addBlockInfo(blockInfo);
		return blockInfo;
	}
	
	public void removeFile(String path) throws IOException {
		File file = new File(path);
		TreeNode<FileNode> node = this.namespace.find(file.getPath());
		if(node!=null){
			if(node.getData().isFile()){
				//TODO remove folders and all its content
				FileInfo fileInfo = this.files.get(file.getPath());
				if(fileInfo==null) throw new IOException("File doesn't exist");
				for(int blockId=0; blockId<fileInfo.getNumBlocks(); blockId++){
					Map<String, BlockInfo> locations = fileInfo.getBlockLocations(blockId);
					for(String local : locations.keySet()){
						NodeInfo nodeInfo = getEnvironment().getSlaveInfo(local);
						if(nodeInfo!=null){
							NodeAddress nodeAddr = new NodeAddress(nodeInfo, Global.getConfiguration().getInt("slave.port"));
							SystemCallReply reply = getSystemCallInterface().request(nodeAddr, "dfs", "removeBlock", locations.get(local));
						}
					}
				}
				this.files.remove(file.getPath());
			}
			this.namespace.remove(file.getPath());
		}
	}

	public Set<String> list(String path) throws IOException {
		Set<String> sortedList = new TreeSet<String>();
		TreeNode<FileNode> node = this.namespace.find(path);
		if(node!=null){
			for(TreeNode<FileNode> child: node.getChildren()){
				sortedList.add(child.getData().getName());
			}
			return sortedList;
		}else return null;
	}

	public SystemCallReply handleSystemCall(SystemCallRequest sysCallMsg){
		Logger.info(sysCallMsg.getModule()+"+"+sysCallMsg.getMethod());
		if("addFile".equals(sysCallMsg.getMethod())){
			Logger.info("adding file");
			String path = Json.loads(sysCallMsg.getJSONValue(), String.class);
			try {
				addFile( path );
			}catch(IOException e){
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,e.getMessage(), SystemCallErrorType.FATAL);
			}
			Logger.info("file added");
			return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null);
		}else if("addFolders".equals(sysCallMsg.getMethod())){
			Logger.info("adding folders");
			String path = Json.loads(sysCallMsg.getJSONValue(), String.class);
			try {
				addFolders( path );
			}catch(IOException e){
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,e.getMessage(), SystemCallErrorType.FATAL);
			}
			Logger.info("folders added");
			return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null);
		}else if("appendBlock".equals(sysCallMsg.getMethod())){
			Logger.info("appending block");
			String fileName = Json.loads(sysCallMsg.getJSONValue(), String.class);
			try {
				BlockInfo blockInfo = appendBlock(fileName);
				Logger.info("block appended");
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),Json.dumps(blockInfo));
			}catch(IOException e){
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,e.getMessage(), SystemCallErrorType.FATAL);
			}
		}else if("addBlock".equals(sysCallMsg.getMethod())){
			Logger.info("adding block");
			SimpleEntry<String, Integer> blockEntry = Json.loads(sysCallMsg.getJSONValue(), new TypeToken<SimpleEntry<String, Integer>>() {}.getType());
			try {
				BlockInfo blockInfo = addBlock(blockEntry.getKey(), blockEntry.getValue().intValue());
				Logger.info("block added");
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),Json.dumps(blockInfo));
			}catch(IOException e){
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,e.getMessage(), SystemCallErrorType.FATAL);
			}
		}else if("updateBlockInfo".equals(sysCallMsg.getMethod())){
			Logger.info("updating block info");
			BlockInfo blockInfo = Json.loads(sysCallMsg.getJSONValue(), BlockInfo.class);
			try {
				updateBlockInfo(blockInfo);
				Logger.info("block added");
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),Json.dumps(blockInfo));
			}catch(IOException e){
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,e.getMessage(), SystemCallErrorType.FATAL);
			}
		}else if("getFileNames".equals(sysCallMsg.getMethod())){
			String jsonValue = Json.dumps(this.files.keySet());
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),jsonValue);
			return reply;
		}else if("list".equals(sysCallMsg.getMethod())){
			String path = Json.loads(sysCallMsg.getJSONValue(), String.class);
			try {
				Set<String> dir = list( path );
				String jsonValue = Json.dumps(dir);
				SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),jsonValue);
				return reply;
			}catch(IOException e){
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,e.getMessage(), SystemCallErrorType.FATAL);
			}			
		}else if("getFileInfo".equals(sysCallMsg.getMethod())){
			Logger.info("getting file info");
			String fileName = Json.loads(sysCallMsg.getJSONValue(), String.class);
			String jsonValue = Json.dumps(this.files.get(fileName));
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),jsonValue);
			Logger.info("done! getting file info");
			return reply;
		}else if("removeFile".equals(sysCallMsg.getMethod())){
			Logger.info("remove file");
			String fileName = Json.loads(sysCallMsg.getJSONValue(), String.class);
			try {
				removeFile(fileName);
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null);
			}catch(IOException e){
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,e.getMessage(), SystemCallErrorType.FATAL);
			}
		}
		return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,"System call unkown", SystemCallErrorType.FATAL);
	}
}
