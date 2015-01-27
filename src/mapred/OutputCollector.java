package mapred;

import java.io.IOException;

public interface OutputCollector<KType, VType> {
	public void collect(KType key, VType value) throws IOException;
	public void close() throws IOException;
}
