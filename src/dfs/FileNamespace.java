package dfs;

import java.io.File;
import java.io.IOException;

import cloudos.util.TreeNode;

public class FileNamespace {
	private TreeNode<FileNode> root;
	
	public FileNamespace(){
		this.root = new TreeNode<FileNode>(new FileNode("", true));
	}

	public TreeNode<FileNode> find(String path){
		if(path.startsWith("/")){
			path = path.substring(1);
			path = (new File(path)).getPath();
			//System.out.println("find:"+path+":");
			String []pathNodes = path.split("/");
			TreeNode<FileNode> current = this.root;
			if(path.length()==0) return this.root;
			for(String node:pathNodes){
				boolean found = false;
				for(TreeNode<FileNode> child: current.getChildren()){
					if(child.getData().getName().equals(node)){
						found = true;
						current = child;
						break;
					}
				}
				if(!found) return null;
			}
			return current;
		}else return null;
	}

	public void addFolder(String path) throws IOException{
		File file = new File(path);
		String parent = file.getParentFile().getPath();
		String name = file.getName();
		if(!FileNode.validateFileName(name)) throw new IOException("Invalid folder name");
		//System.out.println("adding:"+name+":in folder:"+parent+":");
		TreeNode<FileNode> current = this.root;
		if(parent.length()>0){
			current = find(parent);
		}
		if(current!=null)
			current.addChild(new TreeNode<FileNode>(new FileNode(name, true)));
		else throw new IOException("Invalid folder path");
	}

	public void addFolders(String path) throws IOException{
		if(path.startsWith("/")){
			path = path.substring(1);
			path = (new File(path)).getPath();
			//System.out.println("find:"+path+":");
			String []pathNodes = path.split("/");
			TreeNode<FileNode> current = this.root;
			if(path.length()==0) return;
			for(String node:pathNodes){
				boolean found = false;
				for(TreeNode<FileNode> child: current.getChildren()){
					if(child.getData().getName().equals(node)){
						found = true;
						current = child;
						break;
					}
				}
				if(!found){
					if(!FileNode.validateFileName(node)) throw new IOException("Invalid folder name");
					TreeNode<FileNode> folder = new TreeNode<FileNode>(new FileNode(node, true));
					current.addChild(folder);
					current = folder;
				}
			}
		}else throw new IOException("Invalid folder path");
	}

	public void addFile(String path) throws IOException{
		if(path.endsWith("/")) throw new IOException("Missing file name");
		File file = new File(path);
		String parent = file.getParentFile().getPath();
		String name = file.getName();
		if(!FileNode.validateFileName(name)) throw new IOException("Invalid file name");
		TreeNode<FileNode> current = this.root;
		if(parent.length()>0){
			current = find(parent);
		}
		if(current!=null)
			current.addChild(new TreeNode<FileNode>(new FileNode(name, false)));
		else throw new IOException("Invalid file path");
	}

	public void remove(String path, boolean force) throws IOException{
		File file = new File(path);
		String parent = file.getParentFile().getPath();
		String name = file.getName();

		TreeNode<FileNode> current = this.root;
		if(parent.length()>0){
			current = find(parent);
		}
		if(current!=null){
			//current.addChild(new TreeNode<FileNode>(new FileNode(name, true)));
			for(int i = 0; i<current.getNumberOfChildren(); i++){
				TreeNode<FileNode> node = current.getChildAt(i);
				if(node!=null && node.getData().getName().equals(name)){
					if(node.getData().isFolder() && node.getNumberOfChildren()>0 && !force){
						throw new IOException("Folder is not empty");
					}else{
						current.removeChildAt(i);
						break;
					}
				}
			}
		}else throw new IOException("Invalid folder path");
	}
	
	public void remove(String path) throws IOException{
		remove(path, false);
	}
}
