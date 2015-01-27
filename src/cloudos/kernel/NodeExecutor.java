package cloudos.kernel;

import java.io.File;
import java.io.IOException;

import java.net.UnknownHostException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;
import net.schmizz.sshj.transport.verification.HostKeyVerifier;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;


import cloudos.kernel.info.NodeInfo;
import cloudos.kernel.info.ModuleExecutorInfo;
import cloudos.kernel.info.ExecutorInfo;

import cloudos.net.NetUtil;

import cloudos.util.Json;
import cloudos.util.Logger;

class NodeExecutor {

	// Suppress default constructor for noninstantiability
	private NodeExecutor() {
		throw new AssertionError();
	}

	static void setup(NodeInfo nodeInfo, NodeInfo mainNodeInfo) throws IOException{
		if(nodeInfo.getSSHInfo()==null){
			Logger.fatal("No SSH info defined!");
		}
		String mainNodeBase64 = Base64.encodeBase64String(StringUtils.getBytesUtf8(Json.dumps(mainNodeInfo)));
		String localNodeBase64 = Base64.encodeBase64String(StringUtils.getBytesUtf8(Json.dumps(nodeInfo)));

		final SSHClient ssh = new SSHClient();
		ssh.loadKnownHosts();
		/*ssh.addHostKeyVerifier(  
			new HostKeyVerifier() {  
				public boolean verify(String arg0, int arg1, PublicKey arg2){
					return true;  // don't bother verifying
				}
			}
		);*/
		ssh.addHostKeyVerifier(new PromiscuousVerifier());
		ssh.connect(nodeInfo.getAddress());
		if(!ssh.isConnected()){
			Logger.fatal("Could NOT connect to "+nodeInfo.getAddress()+" in "+ssh.getTimeout());
			return;
		}
		try{
			if(nodeInfo.getSSHInfo().getIdentity()!=null)
				ssh.authPublickey(nodeInfo.getSSHInfo().getUser(), nodeInfo.getSSHInfo().getIdentity());
			else if(nodeInfo.getSSHInfo().getPassword()!=null)
				ssh.authPassword(nodeInfo.getSSHInfo().getUser(), nodeInfo.getSSHInfo().getPassword());
			else ssh.authPublickey(nodeInfo.getSSHInfo().getUser());

			ssh.newSCPFileTransfer().upload("./cloudos.tar.gz", nodeInfo.getWorkspace());
			
			Session session = ssh.startSession();
			Logger.info("killing previous executions");
			Command cmd = session.exec("pkill -f 'java.*cloudos.kernel.NodeManager'");
			//System.out.println(IOUtils.readFully(cmd.getInputStream()).toString());
			cmd.join();
			System.out.println("** exit status: " + cmd.getExitStatus());
			session.close();
			
			session = ssh.startSession();
			Logger.info("extracting files");
			cmd = session.exec("tar -zxvf "+nodeInfo.getWorkspace()+"cloudos.tar.gz"+" -C "+nodeInfo.getWorkspace());
			//System.out.println(IOUtils.readFully(cmd.getInputStream()).toString());
			cmd.join();
			System.out.println("** exit status: " + cmd.getExitStatus());
			session.close();

			session = ssh.startSession();
			Logger.info("starting node "+nodeInfo.getName());
			cmd = session.exec("cd "+nodeInfo.getWorkspace()+" && ./run.sh cloudos.kernel.NodeManager -mn "+mainNodeBase64+" -ln "+localNodeBase64);
			//System.out.println(IOUtils.readFully(cmd.getInputStream()).toString());
			cmd.join();
			System.out.println("** exit status: " + cmd.getExitStatus());
			session.close();

			Logger.info("DONE starting node "+nodeInfo.getName());
		}finally{
			ssh.disconnect();
		}
	}

	static void execute(SystemCallInterface sysCall, String name, ExecutorInfo executorInfo, NodeInfo nodeInfo, NodeInfo masterInfo) throws IOException{
		ModuleExecutorInfo moduleExecutorInfo = new ModuleExecutorInfo(name, executorInfo);
		//moduleExecutorInfo.setMasterInfo(masterInfo); //TODO make sure it is not needed

		/*String json = Json.dumps(moduleExecutorInfo);
		SystemCallRequest sysCallMsg = new SystemCallRequest(NodeManager.class.getSimpleName(), "execute", json);
		NodeCommunicator comm = new NodeCommunicator(nodeInfo);
		try{
			comm.connect(Global.getConfiguration().getInt("slave.port")); //get slave.port from the global configuration
			comm.writeLine(Base64.encodeBase64String(StringUtils.getBytesUtf8( Json.dumps(sysCallMsg) )));
			String base64 = comm.readLine();
			json = StringUtils.newStringUtf8(Base64.decodeBase64(base64));
			SystemCallReply reply = Json.loads(json, SystemCallReply.class);
			if(reply.getErrorMessage()!=null){
				Logger.warning(reply.getErrorMessage());
			}
			comm.close();
		}catch(IOException e){
			e.printStackTrace();
		}*/
		NodeAddress nodeAddr = new NodeAddress(nodeInfo, Global.getConfiguration().getInt("slave.port"));
		SystemCallReply reply = sysCall.request(nodeAddr, NodeManager.class.getSimpleName(), "execute", moduleExecutorInfo);
		if(reply.getErrorMessage()!=null){
			//Logger.warning(reply.getErrorMessage());
			throw new IOException(reply.getErrorMessage());
		}
	}

	static void stop(String name, ExecutorInfo executorInfo, NodeInfo nodeInfo){
		
	}
}
