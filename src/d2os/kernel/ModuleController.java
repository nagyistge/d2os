package d2os.kernel;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;


import java.nio.charset.Charset;

import java.net.UnknownHostException;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.io.FileUtils;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import org.apache.zookeeper.KeeperException;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkException;

import d2os.kernel.info.ModuleInfo;
import d2os.kernel.info.NodeInfo;

import d2os.scheduler.LessBusyScheduler;

import d2os.net.NetUtil;

import d2os.io.StandardOutputStream;

import d2os.util.Logger;
import d2os.util.Json;

class ModuleController extends ModuleMaster {
	private ZkClient zk;

	private Map<String, ModuleInfo> modules;

	ModuleController(ZkClient zk){
		modules = new HashMap<String, ModuleInfo>();
		this.zk = zk;

		addSystemCallHandler("getModuleNames", new SystemCallHandler(){
			public SystemCallReply handle(SystemCallContext ctx, SystemCallArguments args){
				return ctx.reply(getModuleNames());
			}
		});
		addSystemCallHandler("getModuleInfo", new SystemCallHandler(){
			public SystemCallReply handle(SystemCallContext ctx, SystemCallArguments args){
				return ctx.reply(getModuleInfo((String)args.get("moduleName")));
			}
		});
		addSystemCallHandler("addModule", new SystemCallHandler(){
			public SystemCallReply handle(SystemCallContext ctx, SystemCallArguments args){
				ModuleInfo moduleInfo = (ModuleInfo)args.get("moduleInfo");
				try{
					addModule(moduleInfo);
				}catch(Exception e){
					return ctx.error(e.getMessage(), SystemCallErrorType.FATAL);
				}
				return ctx.reply(null);
			}
		});
		addSystemCallHandler("removeModule", new SystemCallHandler(){
			public SystemCallReply handle(SystemCallContext ctx, SystemCallArguments args){
				String moduleName = (String)args.get("moduleName");
				try{
					removeModule(moduleName);
				}catch(Exception e){
					return ctx.error(e.getMessage(), SystemCallErrorType.FATAL);
				}
				return ctx.reply(null);
			}
		});
	}

	Set<String> getModuleNames(){
		return modules.keySet();
	}

	ModuleInfo getModuleInfo(String name){
		return modules.get(name);
	}
	
	@Override
	public void onSlaveDead(String nodeName){
		Logger.info("Slave dead: "+nodeName);
	}
	
	@Override
	public void onSlaveAlive(String nodeName){
		Logger.info("Slave alive: "+nodeName);
	}

	@Override
	public void onMasterDead(String nodeName){
		Logger.info("Master dead: "+nodeName);
	}

	@Override	
	public void onMasterAlive(String nodeName){
		Logger.info("Master alive: "+nodeName);
	}

	//SYSTEM CALLS
	void addModule(ModuleInfo moduleInfo) throws IOException, ZkInterruptedException, IllegalArgumentException, ZkException, RuntimeException {
		Logger.info("Adding module: "+moduleInfo.getName());
		
	}

	public void removeModule(String moduleName) throws IllegalArgumentException{
		Logger.info("Removing module: "+moduleName);
		
	}
}
