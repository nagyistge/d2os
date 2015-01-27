package cloudos.kernel;

import org.junit.* ;
import static org.junit.Assert.* ;

public class SystemCallReplyTest {
	@Test
	public void testSimpleConstructor(){
		SystemCallReply req = new SystemCallReply("moduleName", "methodName", "JSONValue");
		assertEquals("moduleName",req.getModule());
		assertEquals("methodName",req.getMethod());
		assertEquals("JSONValue",req.getJSONValue());
		assertNull(req.getErrorMessage());
		assertEquals(SystemCallErrorType.NONE,req.getErrorType());
	}

	@Test
	public void testErrorConstructor(){
		SystemCallReply req = new SystemCallReply("moduleName", "methodName", null, "ERROR Message", SystemCallErrorType.FATAL);
		assertEquals("moduleName",req.getModule());
		assertEquals("methodName",req.getMethod());
		assertNull(req.getJSONValue());
		assertEquals("ERROR Message", req.getErrorMessage());
		assertEquals(SystemCallErrorType.FATAL,req.getErrorType());
	}
}
