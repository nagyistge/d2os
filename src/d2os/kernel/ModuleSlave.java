package d2os.kernel;

public abstract class ModuleSlave extends ModuleExecutor{
	SystemCallReply receiveSystemCallRequest(SystemCallRequest sysCallMsg){
		return handleSystemCall(sysCallMsg);
	}
}
