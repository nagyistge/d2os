package dpm;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import java.math.BigInteger;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.FileReader;

import java.net.MalformedURLException;
import java.net.Socket;
import java.net.ServerSocket;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import com.google.common.net.HostAndPort;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.io.FileUtils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;

import org.apache.zookeeper.KeeperException;

import cloudos.kernel.info.ExecutorInfo;

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
import cloudos.util.ClassUtil;

import cloudos.io.StandardOutputStream;

class ExecutorThread<ExecutorType extends DefaultExecutor>  extends Thread {
	private ExecutorType executor;

	public ExecutorThread(ExecutorType executor){
		this.executor = executor;
	}

	public void run(){
		this.executor.start();
		//this.executor.finish();
	}

	public ExecutorType getExecutor(){
		return this.executor;
	}
}

public class Slave extends ModuleSlave {

	private Map<BigInteger, ExecutorThread<DefaultExecutor>> executors;
	private ExecutorService serverExecutor;

	public void start(){
		super.start();
		this.executors = new HashMap<BigInteger, ExecutorThread<DefaultExecutor>>();
		this.serverExecutor = Executors.newCachedThreadPool();
	}
		
	public void finish(){
		super.finish();
	}

	public void runMasterProcess(Container container){
		ExecutorInfo executorInfo = container.getExecutorInfo();

		String path = null;
		if(executorInfo.getFileBase64()!=null){
			path = RandomStringUtils.randomAlphanumeric(16)+".jar";
			try{
				executorInfo.writeFile(path);
			}catch(IOException e){
				e.printStackTrace();
			}
		}

		MasterProcess masterProcess = null;
		try{
			masterProcess = (MasterProcess)ClassUtil.load(executorInfo.getClassName(), path).newInstance();
		}catch(MalformedURLException e){
			e.printStackTrace();
		}catch(ClassNotFoundException e){
			e.printStackTrace();
		}catch(InstantiationException e){
			e.printStackTrace();
		}catch(IllegalAccessException e){
			e.printStackTrace();
		}

		if(masterProcess==null){
			//throw exception
		}

		masterProcess.setContainerController(new ContainerController(container.getProcessId(), getSystemCallInterface()));
		masterProcess.setConfiguration(container.getConfiguration());
		ExecutorThread<DefaultExecutor> handler = new ExecutorThread<DefaultExecutor>(masterProcess);
		this.executors.put(container.getId(), handler);
		this.serverExecutor.execute( handler );
	}

	public void runBasicProcess(Container container){
		ExecutorInfo executorInfo = container.getExecutorInfo();

		String path = null;
		if(executorInfo.getFileBase64()!=null){
			path = RandomStringUtils.randomAlphanumeric(16)+".jar";
			try{
				executorInfo.writeFile(path);
			}catch(IOException e){
				e.printStackTrace();
			}
		}

		BasicProcess basicProcess = null;
		try{
			basicProcess = (BasicProcess)ClassUtil.load(executorInfo.getClassName(), path).newInstance();
		}catch(MalformedURLException e){
			e.printStackTrace();
		}catch(ClassNotFoundException e){
			e.printStackTrace();
		}catch(InstantiationException e){
			e.printStackTrace();
		}catch(IllegalAccessException e){
			e.printStackTrace();
		}

		if(basicProcess==null){
			//throw exception
		}

		/*
		masterProcess.setContainerController(new ContainerController(container.getProcessId(), getSystemCallInterface()));
		*/
		ExecutorThread<DefaultExecutor> handler = new ExecutorThread<DefaultExecutor>(basicProcess);
		this.executors.put(container.getId(), handler);
		this.serverExecutor.execute( handler );
	}

	public void killProcess(BigInteger containerId){
		ExecutorThread<DefaultExecutor> handler = this.executors.get(containerId);
		if(handler!=null){
			handler.interrupt(); //end thread execution
			handler.getExecutor().finish();
		}
	}
	
	public SystemCallReply handleSystemCall(SystemCallRequest sysCallMsg){
		if("runBasicProcess".equals(sysCallMsg.getMethod())){
			Logger.info("execuring process");
			Container container = Json.loads(sysCallMsg.getJSONValue(), Container.class);
			//ExecutorInfo executorInfo = container.getExecutorInfo();
			try {
				//start container.getId() thread with executorInfo
				runBasicProcess(container);
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null);
			}catch(Exception e){
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,e.getMessage(), SystemCallErrorType.FATAL);
			}
		}else if("runMasterProcess".equals(sysCallMsg.getMethod())){
			Logger.info("execuring process");
			Container container = Json.loads(sysCallMsg.getJSONValue(), Container.class);
			//ExecutorInfo executorInfo = container.getExecutorInfo();
			try {
				//start container.getId() thread with executorInfo
				runMasterProcess(container);
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null);
			}catch(Exception e){
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,e.getMessage(), SystemCallErrorType.FATAL);
			}
		}else if("kill".equals(sysCallMsg.getMethod())){
			Logger.info("killing process");
			Container container = Json.loads(sysCallMsg.getJSONValue(), Container.class);
			try {
				//kill container.getId() thread
				killProcess(container.getId());
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null);
			}catch(Exception e){
				return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,e.getMessage(), SystemCallErrorType.FATAL);
			}
		}
		return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,"System call unkown", SystemCallErrorType.FATAL);
	}
}
