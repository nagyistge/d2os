package watershed.core.api;

import java.io.IOException;

import java.util.Set;
import java.util.Map;

import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;

import org.apache.zookeeper.KeeperException;

import cloudos.kernel.info.NodeInfo;

import cloudos.kernel.SystemCallRequest;
import cloudos.kernel.SystemCallReply;
import cloudos.kernel.SystemCallException;
import cloudos.kernel.NodeAddress;
import cloudos.kernel.NodeCommunicator;
import cloudos.kernel.Global;
import cloudos.kernel.SystemCallInterface;
import cloudos.kernel.ResourceManagerInterface;

import cloudos.util.Json;
import cloudos.util.Logger;


import watershed.core.info.ModuleInfo;

public class WatershedInterface {
	private SystemCallInterface sysCall;
	/*
	public WatershedInterface(NodeInfo nodeInfo){
		this.sysCall = new SystemCallInterface(nodeInfo, Global.getConfiguration().getInt("slave.port"));
	}
	*/
	public WatershedInterface(SystemCallInterface sysCall){
		this.sysCall = sysCall;
	}

	public void load(ModuleInfo moduleInfo) throws IOException, KeeperException, InterruptedException {
		Logger.info("Loading module: "+moduleInfo.getFilterInfo().getName());
		SystemCallReply reply = this.sysCall.request("watershed", "execute", moduleInfo);
		Logger.info("DONE Loading!");
		if(reply.getErrorMessage()!=null){
			throw new SystemCallException(reply.getErrorMessage());
		}
	}
	
	public Set<String> getModuleNames() throws IOException, KeeperException, InterruptedException {
		SystemCallReply reply = this.sysCall.request("watershed", "getModuleNames", null);
		Set<String> moduleNames = null;
		if(reply.getJSONValue()!=null){
			moduleNames = Json.loads(reply.getJSONValue(), new TypeToken< Set<String> >() {}.getType());
		}else if(reply.getErrorMessage()!=null){
			throw new SystemCallException(reply.getErrorMessage());
		}
		return moduleNames;
	}

	public void remove(String moduleName) throws IOException, KeeperException, InterruptedException {
                Logger.info("Removing module: "+moduleName);
                SystemCallReply reply = this.sysCall.request("watershed", "remove", moduleName);
                Logger.info("DONE Removing!");
                if(reply.getErrorMessage()!=null){
                        throw new SystemCallException(reply.getErrorMessage());
                }
        }

}
