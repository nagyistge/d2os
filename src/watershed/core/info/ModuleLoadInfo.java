package watershed.core.info;

public class ModuleLoadInfo {
	private int instance;
	private ModuleInfo moduleInfo;
	
	public ModuleLoadInfo(ModuleInfo moduleInfo, int instance){
		this.moduleInfo = moduleInfo;
		this.instance = instance;
	}

	public int getInstance(){
		return this.instance;
	}

	public void setInstance(int instance){
		this.instance = instance;
	}

	public ModuleInfo getModuleInfo(){
		return this.moduleInfo;
	}

	public void setModuleInfo(ModuleInfo moduleInfo){
		this.moduleInfo = moduleInfo;
	}
}
