package watershed.core.info;

import java.io.IOException;
import java.io.File;

import java.util.Map;
//import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import cloudos.kernel.info.ExecutorInfo;

public class StubInfo extends ExecutorInfo{	
	//private ExecutorInfo executorInfo;
	//private String className;
	//private String fileName;
	private Map<String, String> attrs;
	
	public StubInfo(String className, File file) throws IOException{
		super(className, file);
		//this.className = className;
		this.attrs = new ConcurrentHashMap<String, String>();
		//this.fileName = null;
	}

	public void setAttribute(String key, String value){
		this.attrs.put(key, value);
	}

	public String getAttribute(String key){
		return this.attrs.get(key);
	}

	public Map<String, String> getAttributes(){
		return this.attrs;
	}
}

