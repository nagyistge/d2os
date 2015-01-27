package watershed.core;

import java.io.File;

import java.util.Deque;
import java.util.ArrayDeque;

public class ControlMessage {
	private String src;
	private String dst;
	private String msg;

	public ControlMessage(String src, String dst, String msg){
		this.src = src;
		this.dst = dst;
		this.msg = msg;
	}
	
	public void setSource(String src){
		this.src = src;
	}

	public String getSource(){
		return this.src;
	}

	public void setDestination(String dst){
		this.dst = dst;
	}

	public String getDestination(){
		return this.dst;
	}

	public void setMessage(String msg){
		this.msg = msg;
	}

	public String getMessage(){
		return this.msg;
	}


	public static Deque<String> parsePath(String str){
		if(str==null) return null;

		Deque<String> path = new ArrayDeque<String>();
		File file = new File(str);
		do {
			//System.out.println(file.getName());
			path.push(file.getName());
			file = file.getParentFile();
		}while(file!=null && file.getName().trim().length()>0);
		return path;
	}
}
