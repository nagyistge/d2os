package cloudos.kernel;

import org.junit.* ;
import static org.junit.Assert.* ;

public class SystemCallRequestTest {
	@Test
	public void testConstructor(){
		SystemCallRequest req = new SystemCallRequest("moduleName", "methodName", "JSONValue");
		assertEquals("moduleName",req.getModule());
		assertEquals("methodName",req.getMethod());
		assertEquals("JSONValue",req.getJSONValue());
	}
}
