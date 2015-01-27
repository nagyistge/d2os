package cloudos.kernel;

import java.io.IOException;

import cloudos.kernel.info.NodeInfo;
import cloudos.kernel.info.ModuleExecutorInfo;
import cloudos.kernel.info.ExecutorInfo;

import cloudos.util.Logger;

class NodeExecutor {

	// Suppress default constructor for noninstantiability
	private NodeExecutor() {
		throw new AssertionError();
	}

	static void setup(NodeInfo nodeInfo, NodeInfo mainNodeInfo) throws IOException{
		Logger.info("Mocked NodeExecutor.setup");
	}

	static void execute(SystemCallInterface sysCall, String name, ExecutorInfo executorInfo, NodeInfo nodeInfo, NodeInfo masterInfo) throws IOException{
		Logger.info("Mocked NodeExecutor.execute");
	}

	static void stop(String name, ExecutorInfo executorInfo, NodeInfo nodeInfo){
		Logger.info("Mocked NodeExecutor.stop");
	}
}
