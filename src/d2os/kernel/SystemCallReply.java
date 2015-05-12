package d2os.kernel;

import java.io.Serializable;

/**
 * 
 */
public class SystemCallReply extends SystemCallMessage implements Serializable{
	private final Object value;
	private final String errorMessage;
	private final SystemCallErrorType errorType;
	
	public SystemCallReply(String module, String method, Object value, String errorMessage, SystemCallErrorType errorType){
		super(module,method);
		this.value = value;
		this.errorMessage = errorMessage;
		this.errorType = errorType;
	}

	public SystemCallReply(String module, String method, Object value){
		this(module,method,value, null, SystemCallErrorType.NONE);
	}

	public Object value(){
		return this.value;
	}

	public String errorMessage(){
		return this.errorMessage;
	}

	public SystemCallErrorType errorType(){
		return this.errorType;
	}

}
