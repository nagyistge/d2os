package cloudos.cli;

public class ShellApplicationInfo {
	private String name;
	private String className;
	private String fileName;

	public ShellApplicationInfo(String name, String className, String fileName){
		this.name = name;
		this.className = className;
		this.fileName = fileName;
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

	public void setFileName(String fileName){
		this.fileName = fileName;
	}

	public String getFileName(){
		return this.fileName;
	}
}
