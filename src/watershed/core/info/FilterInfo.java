package watershed.core.info;

import java.io.IOException;
import java.io.File;

import java.util.Map;
//import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import cloudos.kernel.info.ExecutorInfo;

import cloudos.util.ClassUtil;

public class FilterInfo extends ExecutorInfo {
	private Map<String, String> attrs;

	public FilterInfo(String name, String className, File file) throws IOException{
		super(className, file);
		setName(name);
		this.attrs = new ConcurrentHashMap<String, String>();
	}

	public void setAttribute(String key, String value){
		this.attrs.put(key, value);
	}

	public String getAttribute(String key){
		return this.attrs.get(key);
	}
	
	public Map<String,String> getAttributes(){
		return this.attrs;
	}
}

