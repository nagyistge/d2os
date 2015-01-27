package cloudos.kernel;

public class SystemCallRequest {
	private final String module;
	private final String method;
	private final String jsonValue;

	public SystemCallRequest(String module, String method, String jsonValue){
		this.module = module;
		this.method = method;
		this.jsonValue = jsonValue;
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
}
