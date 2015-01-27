package cloudos.kernel;

//import cloudos.kernel.info.NodeInfo;

public abstract class ModuleExecutor extends DefaultExecutor {
	//private NodeInfo masterInfo;
	
	public abstract SystemCallReply handleSystemCall(SystemCallRequest sysCallMsg);

	abstract SystemCallReply receiveSystemCallRequest(SystemCallRequest sysCallMsg);
	/*
	public void setMasterInfo(NodeInfo masterInfo){
		this.masterInfo = masterInfo;
	}																																																																																																																																																																							
	
	public NodeInfo getMasterInfo(){
		return this.masterInfo;
	}
	*/
}
