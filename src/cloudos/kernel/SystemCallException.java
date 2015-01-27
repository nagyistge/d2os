package cloudos.kernel;

import java.io.IOException;

public class SystemCallException extends IOException{
	
	public SystemCallException(String msg){
		super(msg);
	}
}
