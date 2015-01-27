package dfs;

import java.io.IOException;

import java.util.Set;
import java.util.Map;

import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;

import org.apache.zookeeper.KeeperException;

import cloudos.kernel.info.NodeInfo;

import cloudos.kernel.SystemCallRequest;
import cloudos.kernel.SystemCallReply;
import cloudos.kernel.NodeAddress;
import cloudos.kernel.NodeCommunicator;

import cloudos.kernel.Global;

import cloudos.kernel.SystemCallInterface;
import cloudos.kernel.ResourceManagerInterface;

import cloudos.util.Json;
import cloudos.util.Logger;

public class DFSInterface {
	private SystemCallInterface sysCall;
	/*
	public DFSInterface(NodeInfo nodeInfo){
		this.sysCall = new SystemCallInterface(nodeInfo, Global.getConfiguration().getInt("slave.port"));
	}
	*/
	public DFSInterface(SystemCallInterface sysCall){
		this.sysCall = sysCall;
	}

	public void remove(String fileName) throws IOException, KeeperException, InterruptedException {
		SystemCallReply reply = this.sysCall.request("dfs", "removeFile", fileName);
		if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
	}

	public void mkdir(String path) throws IOException, KeeperException, InterruptedException {
		SystemCallReply reply = this.sysCall.request("dfs", "addFolders", path);
		if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
	}

	public Set<String> list() throws IOException, KeeperException, InterruptedException {
		return list("/");
	}

	public Set<String> list(String path) throws IOException, KeeperException, InterruptedException {
		SystemCallReply reply = this.sysCall.request("dfs", "list", path);
		if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
		Set<String> files = Json.loads(reply.getJSONValue(), new TypeToken< Set<String> >() {}.getType());
		return files;
	}

	public FileDescriptor open(String fileName, FileMode mode) throws IOException, KeeperException, InterruptedException {
		if(mode==FileMode.READER)
			return openReader(fileName);
		else if(mode==FileMode.WRITER)
			return openWriter(fileName);
		else throw new IOException("FileMode.APPEND is not supported yet.");
	}
	/*
	public FileDescriptor open(String fileName, FileMode mode, long seek) throws IOException, KeeperException, InterruptedException {
		if(mode==FileMode.READER)
			return openReader(fileName, seek);
		else throw new IOException("FileMode.APPEND is not supported yet.");
	}
	*/

	private long getFileSize(FileInfo fileInfo){
		long size = 0;
		for(int blockId=0;blockId<fileInfo.getNumBlocks();blockId++){
			size += (long)fileInfo.getBlockInfo(blockId).getBlockSize();
		}
		return size;
	}
	
	private FileReaderDescriptor openReader(String fileName) throws IOException, KeeperException, InterruptedException {
		Logger.info("dfs.getFileInfo");
		SystemCallReply reply = this.sysCall.request("dfs", "getFileInfo", fileName);
		if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
		FileInfo fileInfo = Json.loads(reply.getJSONValue(), FileInfo.class);
		if(fileInfo==null) throw new IOException("File does not exist");
		Logger.info("Total file size: "+getFileSize(fileInfo));
		FileReaderDescriptor fd = new FileReaderDescriptor(fileInfo);
		BlockInfo blockInfo = fileInfo.getBlockInfo(0);
		Logger.info("resourceManager.getSlaveInfo");
		NodeInfo nodeInfo = (new ResourceManagerInterface(this.sysCall)).getEnvironmentAlive().getSlaveInfo(blockInfo.getNodeName());
		Logger.info("dfs.readBlock");
		//SystemCallInterface sCall = new SystemCallInterface(nodeInfo, Global.getConfiguration().getInt("slave.port"));
		NodeAddress nodeAddr = new NodeAddress(nodeInfo, Global.getConfiguration().getInt("slave.port"));
		reply = this.sysCall.request(nodeAddr, "dfs", "readBlock", blockInfo);
		if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
		nodeAddr = Json.loads(reply.getJSONValue(), NodeAddress.class);
		fd.setNodeAddress(nodeAddr);
		NodeCommunicator comm = new NodeCommunicator(nodeAddr.getNodeInfo());
		fd.setNodeCommunicator(comm);
		comm.connect(nodeAddr.getPort());
		return fd;
	}
/*
	private FileReaderDescriptor openReader(String fileName, long seekPos) throws IOException, KeeperException, InterruptedException {
		Logger.info("dfs.getFileInfo");
		SystemCallReply reply = this.sysCall.request("dfs", "getFileInfo", fileName);
		if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
		FileInfo fileInfo = Json.loads(reply.getJSONValue(), FileInfo.class);
		if(fileInfo==null) throw new IOException("File does not exist");
		
		long size = 0;
		long pad = 0;
		int currentBlockId = -1;
		for(int blockId=0;blockId<fileInfo.getNumBlocks();blockId++){
			long bSize = (long)fileInfo.getBlockInfo(blockId).getBlockSize();
			if(seekPos<(size+bSize)){
				currentBlockId = blockId;
				pad = seekPos-size;
				break;
			}
			size += bSize;
		}

		Logger.info("Total file size: "+getFileSize(fileInfo));
		FileReaderDescriptor fd = new FileReaderDescriptor(fileInfo);
		fd.setCurrentBlockId(currentBlockId);
		BlockInfo blockInfo = fileInfo.getBlockInfo(currentBlockId);
		Logger.info("resourceManager.getSlaveInfo");
		NodeInfo nodeInfo = (new ResourceManagerInterface(this.sysCall)).getEnvironmentAlive().getSlaveInfo(blockInfo.getNodeName());
		Logger.info("dfs.readBlock");
		//SystemCallInterface sCall = new SystemCallInterface(nodeInfo, Global.getConfiguration().getInt("slave.port"));
		NodeAddress nodeAddr = new NodeAddress(nodeInfo, Global.getConfiguration().getInt("slave.port"));
		reply = this.sysCall.request(nodeAddr, "dfs", "readBlock", blockInfo);
		if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
		nodeAddr = Json.loads(reply.getJSONValue(), NodeAddress.class);
		fd.setNodeAddress(nodeAddr);
		NodeCommunicator comm = new NodeCommunicator(nodeAddr.getNodeInfo());
		fd.setNodeCommunicator(comm);
		comm.connect(nodeAddr.getPort());

		for(int b = 0; b<pad; b++) read(fd);

		return fd;
	}
*/

	public long size(FileDescriptor fd) throws IOException{
		if(fd.getMode()!=FileMode.READER) throw new IOException("File is not open as a reader");
		return getFileSize(((FileReaderDescriptor)fd).getFileInfo());
	}

	public void seek(FileDescriptor fd, long pos) throws IOException, KeeperException, InterruptedException {
		if(fd.getMode()!=FileMode.READER) throw new IOException("File is not open as a reader");
		seek((FileReaderDescriptor)fd,pos);
	}

	public void seek(FileReaderDescriptor fd, long pos) throws IOException, KeeperException, InterruptedException {
		long size = 0;
		long pad = 0;
		int currentBlockId = -1;
		for(int blockId=0;blockId<fd.getFileInfo().getNumBlocks();blockId++){
			long bSize = (long)fd.getFileInfo().getBlockInfo(blockId).getBlockSize();
			if(pos<(size+bSize)){
				currentBlockId = blockId;
				pad = pos-size;
				break;
			}
			size += bSize;
		}

		fd.setCurrentBlockId(currentBlockId);
		BlockInfo blockInfo = fd.getFileInfo().getBlockInfo(currentBlockId);
		Logger.info("resourceManager.getSlaveInfo");
		NodeInfo nodeInfo = (new ResourceManagerInterface(this.sysCall)).getEnvironmentAlive().getSlaveInfo(blockInfo.getNodeName());
		Logger.info("dfs.readBlock");
		//SystemCallInterface sCall = new SystemCallInterface(nodeInfo, Global.getConfiguration().getInt("slave.port"));
		NodeAddress nodeAddr = new NodeAddress(nodeInfo, Global.getConfiguration().getInt("slave.port"));
		SystemCallReply reply = this.sysCall.request(nodeAddr, "dfs", "readBlock", blockInfo);
		if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
		nodeAddr = Json.loads(reply.getJSONValue(), NodeAddress.class);
		fd.setNodeAddress(nodeAddr);
		NodeCommunicator comm = new NodeCommunicator(nodeAddr.getNodeInfo());
		fd.setNodeCommunicator(comm);
		comm.connect(nodeAddr.getPort());
		
		for(int b = 0; b<pad; b++) read(fd);
	}

	public String readLine(FileDescriptor fd) throws IOException, KeeperException, InterruptedException {
		if(fd.getMode()!=FileMode.READER) throw new IOException("File is not open as a reader");
		return readLine((FileReaderDescriptor)fd);
	}

	private String readLine(FileReaderDescriptor fd) throws IOException, KeeperException, InterruptedException {
		String line = fd.getNodeCommunicator().readLine();
		if(line==null){
			fd.setCurrentBlockId(fd.getCurrentBlockId()+1);
			Logger.info("reading block: "+fd.getCurrentBlockId());
			if(fd.getCurrentBlockId()<fd.getFileInfo().getNumBlocks()){
				BlockInfo blockInfo = fd.getFileInfo().getBlockInfo(fd.getCurrentBlockId());
				Logger.info("resourceManager.getSlaveInfo");
				NodeInfo nodeInfo = (new ResourceManagerInterface(this.sysCall)).getEnvironmentAlive().getSlaveInfo(blockInfo.getNodeName());
				Logger.info("dfs.readBlock");
				//SystemCallInterface sCall = new SystemCallInterface(nodeInfo, Global.getConfiguration().getInt("slave.port"));
				NodeAddress nodeAddr = new NodeAddress(nodeInfo, Global.getConfiguration().getInt("slave.port"));
				SystemCallReply reply = this.sysCall.request(nodeAddr, "dfs", "readBlock", blockInfo);
				if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
				nodeAddr = Json.loads(reply.getJSONValue(), NodeAddress.class);
				fd.setNodeAddress(nodeAddr);
				NodeCommunicator comm = new NodeCommunicator(nodeAddr.getNodeInfo());
				fd.setNodeCommunicator(comm);
				comm.connect(nodeAddr.getPort());
				line = comm.readLine();
			}
		}
		return line;
	}

	public int read(FileDescriptor fd) throws IOException, KeeperException, InterruptedException {
		if(fd.getMode()!=FileMode.READER) throw new IOException("File is not open as a reader");
		return read((FileReaderDescriptor)fd);
	}

	private int read(FileReaderDescriptor fd) throws IOException, KeeperException, InterruptedException {
		int charRead = fd.getNodeCommunicator().read();
		if(charRead<0){
			fd.setCurrentBlockId(fd.getCurrentBlockId()+1);
			Logger.info("reading block: "+fd.getCurrentBlockId());
			if(fd.getCurrentBlockId()<fd.getFileInfo().getNumBlocks()){
				BlockInfo blockInfo = fd.getFileInfo().getBlockInfo(fd.getCurrentBlockId());
				Logger.info("resourceManager.getSlaveInfo");
				NodeInfo nodeInfo = (new ResourceManagerInterface(this.sysCall)).getEnvironmentAlive().getSlaveInfo(blockInfo.getNodeName());
				Logger.info("dfs.readBlock");
				//SystemCallInterface sCall = new SystemCallInterface(nodeInfo, Global.getConfiguration().getInt("slave.port"));
				NodeAddress nodeAddr = new NodeAddress(nodeInfo, Global.getConfiguration().getInt("slave.port"));
				SystemCallReply reply = this.sysCall.request(nodeAddr, "dfs", "readBlock", blockInfo);
				if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
				nodeAddr = Json.loads(reply.getJSONValue(), NodeAddress.class);
				fd.setNodeAddress(nodeAddr);
				NodeCommunicator comm = new NodeCommunicator(nodeAddr.getNodeInfo());
				fd.setNodeCommunicator(comm);
				comm.connect(nodeAddr.getPort());
				charRead = comm.read();
			}
		}
		return charRead;
	}
	private void close(FileReaderDescriptor fd) throws IOException {
		fd.getNodeCommunicator().close();
		fd.setCurrentBlockId(0);
		fd.setNodeAddress(null);
		fd.setNodeCommunicator(null);
	}

	private FileWriterDescriptor openWriter(String fileName) throws IOException, KeeperException, InterruptedException {
		Logger.info("dfs.addFile");
		SystemCallReply reply = this.sysCall.request("dfs", "addFile", fileName);
		if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
		FileWriterDescriptor fd = new FileWriterDescriptor(fileName);
		Logger.info("dfs.appendBlock");
		reply = this.sysCall.request("dfs", "appendBlock", fileName);
		if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
		fd.setNumBlocks(fd.getNumBlocks()+1);
		BlockInfo blockInfo = Json.loads(reply.getJSONValue(), BlockInfo.class);
		Logger.info("resourceManager.getSlaveInfo");
		NodeInfo nodeInfo = (new ResourceManagerInterface(this.sysCall)).getEnvironmentAlive().getSlaveInfo(blockInfo.getNodeName());
		Logger.info("dfs.addBlock");
		//SystemCallInterface sCall = new SystemCallInterface(nodeInfo, Global.getConfiguration().getInt("slave.port"));
		NodeAddress nodeAddr = new NodeAddress(nodeInfo, Global.getConfiguration().getInt("slave.port"));
		reply = this.sysCall.request(nodeAddr, "dfs", "addBlock", blockInfo);
		if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
		nodeAddr = Json.loads(reply.getJSONValue(), NodeAddress.class);
		fd.setNodeAddress(nodeAddr);
		NodeCommunicator comm = new NodeCommunicator(nodeAddr.getNodeInfo());
		fd.setNodeCommunicator(comm);
		comm.connect(nodeAddr.getPort());
		fd.setCurrentBlockSize(0);
		Logger.info("file opened");
		return fd;
	}


	public void write(FileDescriptor fd, String data) throws IOException, KeeperException, InterruptedException {
		if(fd.getMode()!=FileMode.WRITER) throw new IOException("File is not open as a writer");
		write((FileWriterDescriptor)fd, data);
	}

	private void write(FileWriterDescriptor fd, String data) throws IOException, KeeperException, InterruptedException {
		//int limit = 8*1024*1024; //TODO get default blocks size in bytes
		if(fd.getCurrentBlockSize()+data.length()<=fd.getBlockSize()){
			fd.setCurrentBlockSize(fd.getCurrentBlockSize()+data.length());
			fd.getNodeCommunicator().writeLine(data);
			fd.getNodeCommunicator().flush();
		}else{
			fd.getNodeCommunicator().close();
			//String data1 = data.substring(0,limit-fd.getCurrentBlockSize());
			//String data2 = data.substring(limit-fd.getCurrentBlockSize());
			//fd.getNodeCommunicator().writeLine(data1);
			//Logger.info("data:*"+data+"*");
			//Logger.info("data1:*"+data1+"*");
			//Logger.info("data2:*"+data2+"*");
			Logger.info("dfs.appendBlock");
			SystemCallReply reply = this.sysCall.request("dfs", "appendBlock", fd.getFileName());
			if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
			fd.setNumBlocks(fd.getNumBlocks()+1);
			BlockInfo blockInfo = Json.loads(reply.getJSONValue(), BlockInfo.class);
			NodeInfo nodeInfo = (new ResourceManagerInterface(this.sysCall)).getEnvironmentAlive().getSlaveInfo(blockInfo.getNodeName());
			Logger.info("dfs.addBlock");
			NodeAddress nodeAddr = new NodeAddress(nodeInfo, Global.getConfiguration().getInt("slave.port"));
			reply = this.sysCall.request(nodeAddr, "dfs", "addBlock", blockInfo);
			if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
			nodeAddr = Json.loads(reply.getJSONValue(), NodeAddress.class);
			fd.setNodeAddress(nodeAddr);
			NodeCommunicator comm = new NodeCommunicator(nodeAddr.getNodeInfo());
			fd.setNodeCommunicator(comm);
			comm.connect(nodeAddr.getPort());
			Logger.info("new block");
			fd.setCurrentBlockSize(data.length());
			fd.getNodeCommunicator().writeLine(data);
		}
	}

	public void flush(FileDescriptor fd) throws IOException {
		fd.getNodeCommunicator().flush();
	}

	private void close(FileWriterDescriptor fd) throws IOException {
		fd.getNodeCommunicator().flush();
		fd.getNodeCommunicator().close();
		fd.setCurrentBlockSize(0);
		fd.setNodeAddress(null);
		fd.setNodeCommunicator(null);
	}

	public void close(FileDescriptor fd) throws IOException {
		if(fd.getMode()==FileMode.WRITER) close((FileWriterDescriptor)fd);
		else if(fd.getMode()==FileMode.READER) close((FileReaderDescriptor)fd);
		else throw new IOException("File mode not supported for closure");
	}
}
