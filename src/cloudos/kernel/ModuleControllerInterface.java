package cloudos.kernel;

import java.util.Set;

import java.io.IOException;

import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;

import org.apache.zookeeper.KeeperException;

import cloudos.kernel.info.ModuleInfo;

import cloudos.util.Json;
import cloudos.util.Logger;

public class ModuleControllerInterface {
	private SystemCallInterface sysCall;

	public ModuleControllerInterface(SystemCallInterface sysCall){
		this.sysCall = sysCall;
	}
	
	public Set<String> getModuleNames() throws IOException, KeeperException, InterruptedException {
		SystemCallReply reply = this.sysCall.request("ModuleController", "getModuleNames", null);
		Set<String> moduleNames = null;
		if(reply.getJSONValue()!=null){
			moduleNames = Json.loads(reply.getJSONValue(), new TypeToken< Set<String> >() {}.getType());
		}else if(reply.getErrorMessage()!=null){
			throw new SystemCallException(reply.getErrorMessage());
		}
		return moduleNames;
	}

	public ModuleInfo getModuleInfo(String moduleName) throws IOException, KeeperException, InterruptedException {
		SystemCallReply reply = this.sysCall.request("ModuleController", "getModuleInfo", moduleName);
		ModuleInfo moduleInfo = null;
		if(reply.getJSONValue()!=null){
			moduleInfo = Json.loads(reply.getJSONValue(), ModuleInfo.class);
		}else if(reply.getErrorMessage()!=null){
			throw new SystemCallException(reply.getErrorMessage());
		}
		return moduleInfo;
	}

	/*
	public SystemCallInterface getModuleSystemCall(String moduleName) throws IOException {
		SystemCallReply reply = this.sysCall.request("ModuleController", "getModuleSystemCall", moduleName);
		SystemCallInterface moduleSysCall = null;
		if(reply.getJSONValue()!=null){
			moduleSysCall = new SystemCallInterface(Json.loads(reply.getJSONValue(), NodeAddress.class));
		}else if(reply.getErrorMessage()!=null){
			throw new SystemCallException(reply.getErrorMessage());
		}
		return moduleSysCall;
	}
	*/
	public void addModule(ModuleInfo moduleInfo) throws IOException, KeeperException, InterruptedException {
		Logger.info("Loading module: "+moduleInfo.getName());
		SystemCallReply reply = this.sysCall.request("ModuleController", "addModule", moduleInfo);
		Logger.info("DONE Loading!");
		if(reply.getErrorMessage()!=null){
			throw new SystemCallException(reply.getErrorMessage());
		}
	}

	public void removeModule(String moduleName) throws IOException, KeeperException, InterruptedException {
		Logger.info("Removing module: "+moduleName);
		SystemCallReply reply = this.sysCall.request("ModuleController", "removeModule", moduleName);
		Logger.info("DONE Removing!");
		if(reply.getErrorMessage()!=null){
			throw new SystemCallException(reply.getErrorMessage());
		}
	}
}
