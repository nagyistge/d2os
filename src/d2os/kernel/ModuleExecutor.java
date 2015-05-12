package d2os.kernel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class ModuleExecutor extends DefaultExecutor {
	private String module;
	private Map<String, SystemCallHandler> sysCallHandlers = new ConcurrentHashMap<String, SystemCallHandler>();
	
	void setModuleName(String module){
		this.module = module;
	}

	public String getModuleName(){
		return this.module;
	}
	
	public void addSystemCallHandler(String name, SystemCallHandler handler){
		sysCallHandlers.put(name, handler);
	}

	public void removeSystemCallHandler(String name){
		sysCallHandlers.remove(name);
	}

	SystemCallReply handleSystemCall(SystemCallRequest req){
		SystemCallReply reply;
		SystemCallContext ctx = new SystemCallContext(req);
		if(this.sysCallHandlers.containsKey(req.method())){
			reply = this.sysCallHandlers.get(req.method()).handle(ctx, req.arguments());
		}else{
			reply = ctx.error("Unknown system call method: "+req.method(), SystemCallErrorType.FATAL);
		}
		return reply;
	}
}
