package cloudos.kernel.info;
/**
 * @author Rodrigo Caetano O. ROCHA
 * @date 23 July 2013
 */

public class SSHInfo {
	private final String user;
	private final String password;
	private final String identity;
	private final int port;
	/*
	public SSHInfo(String user, String password, String identity, int port){
		this.user = user;
		this.password = password;
		this.port = port;
		this.identity = identity;
	}
	*/
	
	public String getUser(){
		return this.user;
	}

	public String getPassword(){
		return this.password;
	}
	
	public String getIdentity(){
		return this.identity;
	}

	public int getPort(){
		return this.port;
	}

	@Override
	public boolean equals(Object o){
		if(!(o instanceof SSHInfo)) return false;
		if(o==this) return true;

		SSHInfo ssh = (SSHInfo)o;

		boolean samePwd = false;
		if(this.password==null && ssh.password==null) samePwd = true;
		else if(this.password!=null && this.password.equals(ssh.password)) samePwd = true;

		boolean sameId = false;
		if(this.identity==null && ssh.identity==null) sameId = true;
		else if(this.identity!=null && this.identity.equals(ssh.identity)) sameId = true;

		return (this.user.equals(ssh.user) && samePwd && sameId && this.port==ssh.port);
	}

	private SSHInfo(Builder builder){
		this.user = builder.user;
		this.password = builder.password;
		this.port = builder.port;
		this.identity = builder.identity;
	}

	public static class Builder {
		private String user;
		private String password;
		private String identity;
		private int port;

		public Builder(String user){
			this.user = user;
			this.port = 22;
		}
	
		public Builder port(int port){
			this.port = port;
			return this;
		}

		public Builder password(String password){
			this.password = password;
			return this;
		}

		public Builder identity(String identity){
			this.identity = identity;
			return this;
		}

		public SSHInfo build(){
			return new SSHInfo(this);
		}
	}
}

