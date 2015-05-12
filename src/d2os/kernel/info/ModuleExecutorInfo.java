package d2os.kernel.info;

public class ModuleExecutorInfo {
	private String name;
	private NodeInfo masterInfo;
	private ExecutorInfo executorInfo;

	public ModuleExecutorInfo(String name, ExecutorInfo executorInfo){
		this.name = name;
		this.executorInfo = executorInfo;
	}

	/*private void setName(String name){
		this.name = name;
	}*/

	public String getName(){
		return this.name;
	}

	/*private void setExecutorInfo(ExecutorInfo executorInfo){
		this.executorInfo = executorInfo;
	}*/

	public ExecutorInfo getExecutorInfo(){
		return this.executorInfo;
	}
	/*
	private void setMasterInfo(NodeInfo masterInfo){
		this.masterInfo = masterInfo;
	}

	public NodeInfo getMasterInfo(){
		return this.masterInfo;
	}
	*/
}
