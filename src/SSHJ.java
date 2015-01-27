
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;
import net.schmizz.sshj.transport.verification.HostKeyVerifier;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import java.security.PublicKey;

public class SSHJ {
	public static void main(String []args) throws IOException {
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
		ssh.connect("localhost");
		try{
			//ssh.authPublickey(System.getProperty("user.name"));
			ssh.authPassword(System.getProperty("user.name"), "31415926");
			final Session session = ssh.startSession();
			try{
				final Command cmd = session.exec("ping -c 1 google.com");
				System.out.println(IOUtils.readFully(cmd.getInputStream()).toString());
				cmd.join(5, TimeUnit.SECONDS);
				System.out.println("\n** exit status: " + cmd.getExitStatus());
			}finally{
				session.close();
			}
		}finally{
			ssh.disconnect();
		}
	}
}
