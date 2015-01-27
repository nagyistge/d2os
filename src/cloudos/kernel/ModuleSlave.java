package cloudos.kernel;

public abstract class ModuleSlave extends ModuleExecutor{
	SystemCallReply receiveSystemCallRequest(SystemCallRequest sysCallMsg){
		return handleSystemCall(sysCallMsg);
	}
}
