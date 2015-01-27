package cloudos.cli;

import cloudos.kernel.SystemCallInterface;

public abstract class ShellApplication {
	
	private SystemCallInterface sysCall;

	public abstract void run(String []args);

	public SystemCallInterface getSystemCallInterface(){
		return this.sysCall;
	}

	void setSystemCallInterface(SystemCallInterface sysCall){
		this.sysCall = sysCall;
	}
}
