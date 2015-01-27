package cloudos.kernel;

interface SystemCallHandler {
	public SystemCallReply handleSystemCall(SystemCallRequest sysCallMsg);
}
