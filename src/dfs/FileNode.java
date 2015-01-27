package dfs;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

class FileNode {
	private String name;
	private boolean isFolder;

	private static Pattern pattern = Pattern.compile("^[\\w\\.][\\w\\. ]*$");

	static boolean validateFileName(String name){
		Matcher m = pattern.matcher(name);
		return m.matches();
	}

	FileNode(String name, boolean isFolder){
		this.name = name;
		this.isFolder = isFolder;
	}

	void setName(String name){
		this.name = name;
	}

	String getName(){
		return this.name;
	}

	boolean isFolder(){
		return this.isFolder;
	}

	boolean isFolder(boolean isFolder){
		this.isFolder = isFolder;
		return this.isFolder;
	}

	boolean isFile(){
		return(!this.isFolder);
	}

	boolean isFile(boolean isFile){
		this.isFolder = !isFile;
		return(!this.isFolder);
	}
	
	public String toString(){
		if(this.isFolder){
			return "<"+this.name+">";
		}else return this.name;
	}
}

