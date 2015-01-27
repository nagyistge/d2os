package cloudos.kernel.info;

import org.junit.* ;
import static org.junit.Assert.* ;

public class NodeInfoTest {
	@Test
	public void testBasicBuilder(){
		NodeInfo node = new NodeInfo.Builder("host", "192.168.0.101").build();
		assertTrue("host".equals(node.getName()) && "192.168.0.101".equals(node.getAddress()) && node.getSSHInfo()==null && node.getWorkspace()==null);
	}

	@Test
	public void testSimpleBuilder(){
		SSHInfo ssh = new SSHInfo.Builder("username").password("password").identity("/home/user/identity.pem").port(8000).build();
		NodeInfo node = new NodeInfo.Builder("host", "192.168.0.101").workspace("/home/user/workspace/").sshInfo(ssh).build();
		assertTrue("host".equals(node.getName()) && "192.168.0.101".equals(node.getAddress()) && ssh.equals(node.getSSHInfo()) && "/home/user/workspace/".equals(node.getWorkspace()));
	}

	@Test
	public void testTrivialEquality(){
		SSHInfo ssh = new SSHInfo.Builder("username").password("password").identity("/home/user/identity.pem").port(8000).build();
		NodeInfo node = new NodeInfo.Builder("host", "192.168.0.101").workspace("/home/user/workspace/").sshInfo(ssh).build();
		assertTrue(node.equals(node));
	}

	@Test
	public void testNullEquality(){
		SSHInfo ssh = new SSHInfo.Builder("username").password("password").identity("/home/user/identity.pem").port(8000).build();
		NodeInfo node = new NodeInfo.Builder("host", "192.168.0.101").workspace("/home/user/workspace/").sshInfo(ssh).build();
		assertFalse(node.equals(null));
	}

	@Test
	public void testBasicEquality(){
		SSHInfo ssh = new SSHInfo.Builder("username").password("password").identity("/home/user/identity.pem").port(8000).build();
		NodeInfo nodeA = new NodeInfo.Builder("host", "192.168.0.101").workspace("/home/user/workspace/").sshInfo(ssh).build();
		NodeInfo nodeB = new NodeInfo.Builder("host", "192.168.0.101").workspace("/home/user/workspace/").sshInfo(ssh).build();
		assertTrue(nodeA.equals(nodeB));
	}

	@Test
	public void testHostInequality(){
		SSHInfo ssh = new SSHInfo.Builder("username").password("password").identity("/home/user/identity.pem").port(8000).build();
		NodeInfo nodeA = new NodeInfo.Builder("host", "192.168.0.101").workspace("/home/user/workspace/").sshInfo(ssh).build();
		NodeInfo nodeB = new NodeInfo.Builder("hostname", "192.168.0.101").workspace("/home/user/workspace/").sshInfo(ssh).build();
		assertFalse(nodeA.equals(nodeB));
	}

	@Test
	public void testAddrInequality(){
		SSHInfo ssh = new SSHInfo.Builder("username").password("password").identity("/home/user/identity.pem").port(8000).build();
		NodeInfo nodeA = new NodeInfo.Builder("host", "192.168.0.101").workspace("/home/user/workspace/").sshInfo(ssh).build();
		NodeInfo nodeB = new NodeInfo.Builder("host", "192.168.0.102").workspace("/home/user/workspace/").sshInfo(ssh).build();
		assertFalse(nodeA.equals(nodeB));
	}

	@Test
	public void testWorkspaceInequality(){
		SSHInfo ssh = new SSHInfo.Builder("username").password("password").identity("/home/user/identity.pem").port(8000).build();
		NodeInfo nodeA = new NodeInfo.Builder("host", "192.168.0.101").workspace("/home/user/workspace/").sshInfo(ssh).build();
		NodeInfo nodeB = new NodeInfo.Builder("host", "192.168.0.101").workspace("/home/user/wrkspc/").sshInfo(ssh).build();
		assertFalse(nodeA.equals(nodeB));
	}

	@Test
	public void testSSHInequality(){
		SSHInfo sshA = new SSHInfo.Builder("username").password("password").identity("/home/user/identity.pem").port(8000).build();
		SSHInfo sshB = new SSHInfo.Builder("user").password("p4sSw0rD").identity("/home/user/identity.pem").port(8000).build();
		NodeInfo nodeA = new NodeInfo.Builder("host", "192.168.0.101").workspace("/home/user/workspace/").sshInfo(sshA).build();
		NodeInfo nodeB = new NodeInfo.Builder("host", "192.168.0.101").workspace("/home/user/workspace/").sshInfo(sshB).build();
		assertFalse(nodeA.equals(nodeB));
	}
}
