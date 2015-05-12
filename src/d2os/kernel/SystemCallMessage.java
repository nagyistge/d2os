package d2os.kernel;

import java.io.Serializable;

/**
 * {@link SystemCallMessage} represents the base class for both the {@link SystemCallRequest} and {@link SystemCallReply}.
 */
public class SystemCallMessage implements Serializable{
	private final String module;
	private final String method;

	public SystemCallMessage(String module, String method){
		this.module = module;
		this.method = method;
	}

	public String module(){
		return this.module;
	}

	public String method(){
		return this.method;
	}
}

