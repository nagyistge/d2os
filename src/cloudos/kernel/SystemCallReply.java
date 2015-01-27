package cloudos.kernel;

public class SystemCallReply {
	private final String module;
	private final String method;
	private final String jsonValue;
	private final String errorMessage;
	private final SystemCallErrorType errorType;
	
	public SystemCallReply(String module, String method, String jsonValue, String errorMessage, SystemCallErrorType errorType){
		this.module = module;
		this.method = method;
		this.jsonValue = jsonValue;
		this.errorMessage = errorMessage;
		this.errorType = errorType;
	}

	public SystemCallReply(String module, String method, String jsonValue){
		this(module,method,jsonValue, null, SystemCallErrorType.NONE);
	}

	public String getModule(){
		return this.module;
	}

	public String getMethod(){
		return this.method;
	}

	public String getJSONValue(){
		return this.jsonValue;
	}

	public String getErrorMessage(){
		return this.errorMessage;
	}

	public SystemCallErrorType getErrorType(){
		return this.errorType;
	}

}
