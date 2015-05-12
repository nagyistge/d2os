package d2os.kernel;

public class DefaultExecutor {
	private boolean isRunning = false;
	private SystemCallInterface sysCall;

	public void start(){
		this.isRunning = true;
	}

	public void finish(){
		this.isRunning = false;
	}

	public boolean isRunning(){
		return this.isRunning;
	}
	
	public SystemCallInterface getSystemCallInterface(){
		return this.sysCall;
	}

	public void setSystemCallInterface(SystemCallInterface sysCall){
		this.sysCall = sysCall;
	}

}
