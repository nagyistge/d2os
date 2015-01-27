package dfs;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.FileReader;

import java.net.Socket;
import java.net.ServerSocket;

import com.google.common.net.HostAndPort;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.io.FileUtils;

import org.apache.zookeeper.KeeperException;

import cloudos.kernel.ModuleSlave;
import cloudos.kernel.info.ExecutorInfo;
import cloudos.kernel.DefaultExecutor;
import cloudos.kernel.SystemCallRequest;
import cloudos.kernel.SystemCallReply;
import cloudos.kernel.SystemCallErrorType;
import cloudos.kernel.NodeAddress;
import cloudos.kernel.SystemCallInterface;

import cloudos.kernel.Global;

import cloudos.net.MessageHandler;
import cloudos.net.NetServer;

import cloudos.util.ThreadExecutor;
import cloudos.util.Logger;
import cloudos.util.Json;

import cloudos.io.StandardOutputStream;

class DFSWriteHandler extends Thread {
	private ServerSocket socket;
	private BlockInfo blockInfo;
	private PrintWriter dataBlock;
	private SystemCallInterface sysCall;

	public DFSWriteHandler(BlockInfo blockInfo, SystemCallInterface sysCall) throws IOException{
		this.sysCall = sysCall;
		this.socket = new ServerSocket(0);
		this.blockInfo = blockInfo;
		File parent = (new File(blockInfo.getPath())).getParentFile();
		if(parent==null) throw new IOException("Invalid block identifier");
		FileUtils.forceMkdir( parent );
		this.dataBlock = new PrintWriter(new BufferedWriter(new FileWriter(blockInfo.getPath())));
	}

	public int getPort(){
		return this.socket.getLocalPort();
	}

	public void run(){
		//int limit = 8*1024*1024; //TODO get default block size in bytes
		int currentBlockSize = 0;
		boolean done = false;
		try{
			Socket socket = this.socket.accept();
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			String inputLine;
			while(true) {
				inputLine = in.readLine();
				if(inputLine == null) {
					break;
				}
				/*
				if((currentBlockSize+inputLine.length()+1)>this.blockInfo.getBlockSize()){
					//inputLine = inputLine.substring(0, this.blockInfo.getBlockSize()-currentBlockSize);
					done = true;
				}
				currentBlockSize += inputLine.length()+1;
				*/
				this.dataBlock.println(inputLine);

				if(done) break;
			}
			this.dataBlock.flush();
			in.close();
			socket.close();
			this.dataBlock.close();
		}catch(IOException e) {
			e.printStackTrace();
		}
		try{
			this.blockInfo.setBlockSize((int)(new File(this.blockInfo.getPath())).length());
			this.sysCall.request("dfs", "updateBlockInfo", this.blockInfo);
		}catch(IOException e) {
			e.printStackTrace();
		}catch(KeeperException e){
			e.printStackTrace();
		}catch(InterruptedException e){
			e.printStackTrace();
		}
	}
}

class DFSReadHandler extends Thread {
	private ServerSocket socket;
	private BlockInfo blockInfo;
	private BufferedReader dataBlock;

	public DFSReadHandler(BlockInfo blockInfo) throws IOException{
		this.socket = new ServerSocket(0);
		this.blockInfo = blockInfo;
		this.dataBlock = new BufferedReader(new FileReader(blockInfo.getPath()));
	}

	public int getPort(){
		return this.socket.getLocalPort();
	}

	public void run(){
		boolean done = false;
		Logger.info("sending block: "+blockInfo.getBlockId());
		try{
			Socket socket = this.socket.accept();
			StandardOutputStream out = new StandardOutputStream(socket.getOutputStream());
			String line;
			while( (line=this.dataBlock.readLine())!=null ){
				out.writeLine(line);
				out.flush();
			}
			Logger.info("done! sending block: "+blockInfo.getBlockId());
			//out.writeLine(null);
			out.flush();
			out.close();
			socket.close();
			this.dataBlock.close();
		}catch(IOException e) {
			e.printStackTrace();
		}
		Logger.info("finished dfs read handler");
	}
}

public class Slave extends ModuleSlave{
	ExecutorService serverExecutor;
	List<BlockInfo> blocks;

	public void start(){
		super.start();
		serverExecutor = Executors.newCachedThreadPool();
		blocks = new ArrayList<BlockInfo>();
	}
		
	public void finish(){
		super.finish();
	}

	private NodeAddress addBlock(BlockInfo blockInfo){
		int port = 0;
		try{
			DFSWriteHandler handler = new DFSWriteHandler(blockInfo, getSystemCallInterface());
			port = handler.getPort();
			serverExecutor.execute( handler );
		}catch(IOException e){
			e.printStackTrace();
		}
		this.blocks.add(blockInfo);

		return new NodeAddress(Global.getLocalNodeInfo(), port);
	}

	private void closeBlock(BlockInfo blockInfo) throws IOException, KeeperException, InterruptedException{
		//get the written block
		/*
		try{
			this.blockInfo.setBlockSize((int)(new File(this.blockInfo.getPath())).length());
			this.sysCall.request("dfs", "updateBlockInfo", this.blockInfo);
		}catch(IOException e) {
			e.printStackTrace();
		}catch(KeeperException e){
			e.printStackTrace();
		}catch(InterruptedException e){
			e.printStackTrace();
		}
		*/
	}

	private NodeAddress readBlock(BlockInfo blockInfo){
		int port = 0;
		try{
			DFSReadHandler handler = new DFSReadHandler(blockInfo);
			port = handler.getPort();
			serverExecutor.execute( handler );
		}catch(IOException e){
			e.printStackTrace();
		}

		return new NodeAddress(Global.getLocalNodeInfo(), port);
	}

	private void removeBlock(BlockInfo blockInfo) throws IOException{
		FileUtils.forceDelete(new File(blockInfo.getPath()));
		(new File(blockInfo.getPath())).getParentFile().delete();
	}

	public SystemCallReply handleSystemCall(SystemCallRequest sysCallMsg){
		if("addBlock".equals(sysCallMsg.getMethod())){
			BlockInfo blockInfo = Json.loads(sysCallMsg.getJSONValue(), BlockInfo.class);
			NodeAddress addr = addBlock(blockInfo);
			return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),Json.dumps(addr));
		}else if("closeBlock".equals(sysCallMsg.getMethod())){
			BlockInfo blockInfo = Json.loads(sysCallMsg.getJSONValue(), BlockInfo.class);
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null);
			try{
				closeBlock(blockInfo);
			}catch(Exception e){
				reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null, e.getMessage(), SystemCallErrorType.FATAL);
			}
			return reply;
		}else if("readBlock".equals(sysCallMsg.getMethod())){
			BlockInfo blockInfo = Json.loads(sysCallMsg.getJSONValue(), BlockInfo.class);
			NodeAddress addr = readBlock(blockInfo);
			return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),Json.dumps(addr));
		}else if("removeBlock".equals(sysCallMsg.getMethod())){
			BlockInfo blockInfo = Json.loads(sysCallMsg.getJSONValue(), BlockInfo.class);
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null);
			try{
				removeBlock(blockInfo);
			}catch(IOException e){
				reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null, e.getMessage(), SystemCallErrorType.FATAL);
			}
			return reply;
		}
		return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,"System call unkown", SystemCallErrorType.FATAL);
	}
}
