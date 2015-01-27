package cloudos.kernel;

import org.junit.* ;
import static org.junit.Assert.* ;

import org.apache.commons.lang3.RandomStringUtils;

import org.xml.sax.SAXException;

import java.io.IOException;

import cloudos.kernel.info.NodeInfo;
import cloudos.kernel.info.SSHInfo;

public class EnvironmentInfoTest {
	
	@Test
	public void testMasters(){
		NodeInfo []nodes = new NodeInfo[10];
		for(int i = 0; i<nodes.length; i++){
			nodes[i] = new NodeInfo.Builder(RandomStringUtils.randomAlphanumeric(10), "192.168.0.10"+i).build();
		}

		Environment env = new Environment();

		for(int i = 0; i<nodes.length; i++){
			env.addMaster(nodes[i]);
			assertTrue( (i+1)==env.getMasters().size());
		}
		
		for(int i = 0; i<nodes.length; i++){
			assertTrue(env.getMasters().contains(nodes[i].getName()));
		}

		for(int i = 0; i<nodes.length; i++){
			assertTrue(env.getMasterInfo(nodes[i].getName()).equals(nodes[i]));
		}

		for(int i = 0; i<nodes.length; i++){
			env.removeMaster(nodes[i].getName());
			assertTrue( (nodes.length-(i+1))==env.getMasters().size());
			for(int j = i+1; j<nodes.length; j++){
				assertTrue(env.getMasters().contains(nodes[j].getName()));
			}
			assertNull(env.getMasterInfo(nodes[i].getName()));
		}
		assertTrue(env.getMasters().size()==0);
	}

	@Test(expected = NullPointerException.class)
	public void testAddNullMaster(){
		Environment env = new Environment();
		env.addMaster(null);
	}

	@Test
	public void testSlaves(){
		NodeInfo []nodes = new NodeInfo[10];
		for(int i = 0; i<nodes.length; i++){
			nodes[i] = new NodeInfo.Builder(RandomStringUtils.randomAlphanumeric(10), "192.168.0.10"+i).build();
		}

		Environment env = new Environment();

		for(int i = 0; i<nodes.length; i++){
			env.addSlave(nodes[i]);
			assertTrue( (i+1)==env.getSlaves().size());
		}
		
		for(int i = 0; i<nodes.length; i++){
			assertTrue(env.getSlaves().contains(nodes[i].getName()));
		}

		for(int i = 0; i<nodes.length; i++){
			assertTrue(env.getSlaveInfo(nodes[i].getName()).equals(nodes[i]));
		}

		for(int i = 0; i<nodes.length; i++){
			env.removeSlave(nodes[i].getName());
			assertTrue( (nodes.length-(i+1))==env.getSlaves().size());
			for(int j = i+1; j<nodes.length; j++){
				assertTrue(env.getSlaves().contains(nodes[j].getName()));
			}
			assertNull(env.getSlaveInfo(nodes[i].getName()));
		}
		assertTrue(env.getSlaves().size()==0);
	}

	@Test(expected = NullPointerException.class)
	public void testAddNullSlave(){
		Environment env = new Environment();
		env.addSlave(null);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void testRemoveFromMasterSet(){
		Environment env = new Environment();
		env.getMasters().remove("anyone");
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testRemoveFromSlaveSet(){
		Environment env = new Environment();
		env.getSlaves().remove("anyone");
	}	

	@Test(expected = UnsupportedOperationException.class)
	public void testAddIntomMasterSet(){
		Environment env = new Environment();
		env.getMasters().add("anyone");
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testAddIntoSlaveSet(){
		Environment env = new Environment();
		env.getSlaves().add("anyone");
	}	
	
	@Test
	public void testLoadFromXML() throws Exception {
		Environment env = Environment.loadFromXMLFile("./tests/environment.xml");

		assertNotNull(env.getMasterInfo("master1"));
		assertTrue( (new NodeInfo.Builder("master1", "192.168.2.101"))
			.workspace("/home/username1/master1/")
			.sshInfo(
				new SSHInfo.Builder("username1").password("p4sSw0rD").identity("/home/username1/identity.pem").port(22).build()
			)
			.build().equals(env.getMasterInfo("master1"))
		);

		assertNotNull(env.getMasterInfo("master2"));
		assertTrue( (new NodeInfo.Builder("master2", "192.168.2.102"))
			.workspace("/home/username2/master2/")
			.sshInfo(
				new SSHInfo.Builder("username2").port(8000).build()
			)
			.build().equals(env.getMasterInfo("master2"))
		);

		assertNotNull(env.getSlaveInfo("slave1"));
		assertTrue( (new NodeInfo.Builder("slave1", "192.168.2.111"))
			.workspace("/home/user1/slave1/")
			.sshInfo(
				new SSHInfo.Builder("user1").password("password").build()
			)
			.build().equals(env.getSlaveInfo("slave1"))
		);

		assertNotNull(env.getSlaveInfo("slave2"));
		assertTrue( (new NodeInfo.Builder("slave2", "192.168.2.112"))
			.workspace("/home/user2/slave2/")
			.sshInfo(
				new SSHInfo.Builder("user2").build()
			)
			.build().equals(env.getSlaveInfo("slave2"))
		);

		assertNotNull(env.getSlaveInfo("slave3"));
		assertTrue( (new NodeInfo.Builder("slave3", "192.168.2.113"))
			.workspace("/home/user3/slave3/")
			.sshInfo(
				new SSHInfo.Builder("user3").build()
			)
			.build().equals(env.getSlaveInfo("slave3"))
		);

		assertNotNull(env.getSlaveInfo("slave4"));
		assertTrue( (new NodeInfo.Builder("slave4", "192.168.2.114"))
			.workspace("/home/user4/slave4/")
			.sshInfo(null)
			.build().equals(env.getSlaveInfo("slave4"))
		);
	}

	@Test(expected = NumberFormatException.class)
	public void testLoadFromXMLNumberFormatException() throws Exception {
		Environment.loadFromXMLFile("./tests/environmentFormatException.xml");
	}

	@Test(expected = IOException.class)
	public void testLoadFromXMLNotFoundException() throws Exception {
		Environment.loadFromXMLFile("./tests/environmentNotFoundException.xml");
	}

	@Test(expected = SAXException.class)
	public void testLoadFromXMLParserException() throws Exception {
		Environment.loadFromXMLFile("./tests/src/cloudos/kernel/EnvironmentInfoTest.java");
	}
}
