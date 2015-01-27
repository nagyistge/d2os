package mapred;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

public class ClientSample {
	public static void main(String []args){
		Configuration conf = new PropertiesConfiguration();
		conf.setProperty("input.path", Mapper.class.getName());
		conf.setProperty("output.path", Mapper.class.getName());
		conf.setProperty("mapper.class", Mapper.class.getName());
		conf.setProperty("mapper.file", null);
		conf.setProperty("reducer.class", Reducer.class.getName());
		conf.setProperty("reducer.file", null);
	}
}
