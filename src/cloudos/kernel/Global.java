package cloudos.kernel;

import org.apache.commons.configuration.Configuration;

import cloudos.kernel.info.NodeInfo;

public class Global {
	private static Configuration configuration;
	private static NodeInfo mainNodeInfo;
	private static NodeInfo localNodeInfo;

	// Suppress default constructor for noninstantiability
	private Global() {
		throw new AssertionError();
	}

	//TODO solve this leak of permission
	public static void setConfiguration(Configuration config){
		configuration = config;
	}

	public static Configuration getConfiguration(){
		return configuration;
	}

	static void setMainNodeInfo(NodeInfo nodeInfo){
		mainNodeInfo = nodeInfo;
	}

	public static NodeInfo getMainNodeInfo(){
		return mainNodeInfo;
	}

	static void setLocalNodeInfo(NodeInfo nodeInfo){
		localNodeInfo = nodeInfo;
	}

	public static NodeInfo getLocalNodeInfo(){
		return localNodeInfo;
	}
	//TODO change master.port to kernel.port, slave.port to module.port or node.port
}
