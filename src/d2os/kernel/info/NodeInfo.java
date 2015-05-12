package d2os.kernel.info;
/**
 * @author Rodrigo Caetano O. ROCHA
 * @date 23 July 2013
 */

public class NodeInfo {
	private final String name;
	private final String address;
	private final String workspace;
	//private int port;
	private final SSHInfo sshInfo;
	
	public String getName(){
		return this.name;
	}

	public String getAddress(){
		return this.address;
	}

	public String getWorkspace(){
		return this.workspace;
	}

	public SSHInfo getSSHInfo(){
		return this.sshInfo;
	}

	private NodeInfo(Builder builder){
		this.name = builder.name;
		this.address = builder.address;
		this.workspace = builder.workspace;
		this.sshInfo = builder.sshInfo;
	}
	
	@Override
	public boolean equals(Object o){
		if(!(o instanceof NodeInfo)) return false;
		if(o==this) return true;

		NodeInfo node = (NodeInfo)o;

		boolean sameWksp = false;
		if(this.workspace==null && node.workspace==null) sameWksp = true;
		else if(this.workspace!=null && this.workspace.equals(node.workspace)) sameWksp = true;

		boolean sameSSH = false;
		if(this.sshInfo==null && node.sshInfo==null) sameSSH = true;
		else if(this.sshInfo!=null && this.sshInfo.equals(node.sshInfo)) sameSSH = true;

		return (this.name.equals(node.name) && this.address.equals(node.address) && sameWksp && sameSSH);
	}
	
	public static class Builder {
		private final String name;
		private final String address;
		private String workspace;
		private SSHInfo sshInfo;

		public Builder(String name, String address){
			this.name = name;
			this.address = address;
			this.workspace = null;
			this.sshInfo = null;
		}
		
		public Builder workspace(String workspace){
			this.workspace = workspace;
			return this;
		}

		public Builder sshInfo(SSHInfo sshInfo){
			this.sshInfo = sshInfo;
			return this;
		}

		public NodeInfo build(){
			return new NodeInfo(this);
		}
	}
}

