package cloudos.kernel.info;

import org.junit.* ;
import static org.junit.Assert.* ;

public class SSHInfoTest {
	@Test
	public void testBasicBuilder(){
		SSHInfo ssh = new SSHInfo.Builder("username").build();
		assertTrue("username".equals(ssh.getUser()) && ssh.getPort()==22 && ssh.getPassword()==null && ssh.getIdentity()==null);
	}

	@Test
	public void testSimpleBuilder(){
		SSHInfo ssh = new SSHInfo.Builder("username").password("password").identity("/home/user/identity.pem").port(8000).build();
		assertTrue("username".equals(ssh.getUser()) && ssh.getPort()==8000 && "password".equals(ssh.getPassword()) && "/home/user/identity.pem".equals(ssh.getIdentity()));
	}

	@Test
	public void testTrivialEquality(){
		SSHInfo ssh = new SSHInfo.Builder("username").password("password").identity("/home/user/identity.pem").port(8000).build();
		assertTrue(ssh.equals(ssh));
	}

	@Test
	public void testNullEquality(){
		SSHInfo ssh = new SSHInfo.Builder("username").password("password").identity("/home/user/identity.pem").port(8000).build();
		assertFalse(ssh.equals(null));
	}

	@Test
	public void testBasicEquality(){
		SSHInfo sshA = new SSHInfo.Builder("username").password("password").identity("/home/user/identity.pem").port(8000).build();
		SSHInfo sshB = new SSHInfo.Builder("username").password("password").identity("/home/user/identity.pem").port(8000).build();
		assertTrue(sshA.equals(sshB));
	}

	@Test
	public void testPortInequality(){
		SSHInfo sshA = new SSHInfo.Builder("username").password("password").identity("/home/user/identity.pem").port(8000).build();
		SSHInfo sshB = new SSHInfo.Builder("username").password("password").identity("/home/user/identity.pem").port(22).build();
		assertFalse(sshA.equals(sshB));
	}

	@Test
	public void testPwdInequality(){
		SSHInfo sshA = new SSHInfo.Builder("username").password("password").identity("/home/user/identity.pem").port(8000).build();
		SSHInfo sshB = new SSHInfo.Builder("username").password("p4sSw0rD").identity("/home/user/identity.pem").port(8000).build();
		assertFalse(sshA.equals(sshB));
	}

@Test
	public void testUserInequality(){
		SSHInfo sshA = new SSHInfo.Builder("username").password("password").identity("/home/user/identity.pem").port(8000).build();
		SSHInfo sshB = new SSHInfo.Builder("user").password("password").identity("/home/user/identity.pem").port(8000).build();
		assertFalse(sshA.equals(sshB));
	}


	@Test
	public void testIdInequality(){
		SSHInfo sshA = new SSHInfo.Builder("username").password("password").identity("/home/user/identity.pem").port(8000).build();
		SSHInfo sshB = new SSHInfo.Builder("username").password("password").identity("/home/user/otherId.pem").port(8000).build();
		assertFalse(sshA.equals(sshB));
	}

}
