package d2os.kernel;

public class SystemCallContext {
	private final String module;
	private final String method;

	SystemCallContext(SystemCallRequest req){
		this(req.module(), req.method());
	}

	SystemCallContext(String module, String method){
		this.module = module;
		this.method = method;
	}

	public SystemCallReply reply(Object value, String errorMessage, SystemCallErrorType errorType){
		return new SystemCallReply(module, method, value, errorMessage, errorType);
	}

	public SystemCallReply reply(Object value){
		return reply(value, null, SystemCallErrorType.NONE);
	}

	public SystemCallReply error(String errorMessage, SystemCallErrorType errorType){
		return reply(null, errorMessage, errorType);
	}
}

