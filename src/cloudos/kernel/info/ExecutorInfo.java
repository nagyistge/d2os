package cloudos.kernel.info;

import java.io.IOException;
import java.io.File;

import org.apache.commons.io.FileUtils;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

public class ExecutorInfo {
	private String name;
	private String className;
	private String fileBase64;

	public ExecutorInfo(String className, File file) throws IOException{
		this.name = null;
		this.className = className;
		if(file!=null)
			this.fileBase64 = Base64.encodeBase64String(FileUtils.readFileToByteArray(file));
		else this.fileBase64 = null;
	}
	
	public void setName(String name){
		this.name = name;
	}
	
	public String getName(){
		return this.name;
	}
	
	public void setClassName(String className){
		this.className = className;
	}
	
	public String getClassName(){
		return this.className;
	}
	
	public void setFileBase64(String fileBase64){
		this.fileBase64 = fileBase64;
	}
	
	public void writeFile(String fileName) throws IOException {
		File file = new File(fileName);
		File parent = file.getParentFile();
		if(parent!=null) FileUtils.forceMkdir( parent );
		FileUtils.writeByteArrayToFile(file, Base64.decodeBase64(getFileBase64()));
	}

	public String getFileBase64(){
		return this.fileBase64;
	}
}
