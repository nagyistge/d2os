package cloudos.kernel;

public class DefaultExecutor {
	private boolean running = false;
	private SystemCallInterface sysCall;

	public void start(){
		this.running = true;
	}

	public void finish(){
		this.running = false;
	}

	public boolean isRunning(){
		return this.running;
	}
	
	public SystemCallInterface getSystemCallInterface(){
		return this.sysCall;
	}

	public void setSystemCallInterface(SystemCallInterface sysCall){
		this.sysCall = sysCall;
	}

}
