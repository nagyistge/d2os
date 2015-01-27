package mapred;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

public interface RecordReader<IKType, IVType>{
	public IKType currentKey();
	public IVType currentValue();
	public boolean nextKeyValue() throws IOException, KeeperException, InterruptedException ;
	public void close() throws IOException, KeeperException, InterruptedException ;
	public float getProgress();
}
