package d2os.kernel;

import java.io.IOException;

import java.util.Map;
import java.util.HashMap;

import d2os.kernel.info.NodeInfo;

import d2os.util.Logger;
import d2os.util.Json;

public class ModuleMaster extends ModuleExecutor {
	private Environment environment;

	void setEnvironment(Environment environment){
		this.environment = environment;
	}

	public Environment getEnvironment(){
		return this.environment;
	}

	public void onSlaveDead(String nodeName){
		Logger.info("slave dead: "+nodeName);
	}
	
	public void onSlaveAlive(String nodeName){
		Logger.info("slave alive: "+nodeName);
	}

	public void onMasterDead(String nodeName){
		Logger.info("master dead: "+nodeName);
	}
	
	public void onMasterAlive(String nodeName){
		Logger.info("master alive: "+nodeName);
	}
}
