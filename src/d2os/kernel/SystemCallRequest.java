package d2os.kernel;

import java.io.Serializable;

/**
 * 
 */
public class SystemCallRequest extends SystemCallMessage implements Serializable{
	private final SystemCallArguments args;
	private final boolean waitReply;

	public SystemCallRequest(String module, String method, SystemCallArguments args, boolean waitReply){
		super(module, method);
		this.args = args;
		this.waitReply = waitReply;
	}

	public SystemCallRequest(String module, String method, SystemCallArguments args){
		this(module, method, args, true);
	}

	public SystemCallRequest(String module, String method){
		this(module,method,new SystemCallArguments());
	}

	public SystemCallArguments arguments(){
		return this.args;
	}

	public boolean waitReply(){
		return this.waitReply;
	}
}

