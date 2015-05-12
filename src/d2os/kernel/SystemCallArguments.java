package d2os.kernel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import java.io.Serializable;

public class SystemCallArguments implements Serializable {
	private Map<String, Object> args;
	
	public SystemCallArguments(){
		this.args = new ConcurrentHashMap<String, Object>();
	}

	public SystemCallArguments add(String name, Object value){
		this.args.put(name, value);
		return this;
	}
	
	public <T> T get(String name, T defaultValue){
		T value = (T)this.args.get(name);
		return (value==null?defaultValue:value);
	}

	public Object get(String name){
		return this.args.get(name);
	}
}
