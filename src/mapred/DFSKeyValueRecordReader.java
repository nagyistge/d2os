package mapred;

import java.io.IOException;

import java.util.AbstractMap.SimpleEntry;

import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;

import org.apache.zookeeper.KeeperException;

import dfs.DFSInterface;
import dfs.FileDescriptor;
import dfs.FileMode;

import cloudos.kernel.SystemCallInterface;
import cloudos.util.Json;

public class DFSKeyValueRecordReader<IKType, IVType> implements RecordReader<IKType, IVType>{
	private DFSInterface dfs;
	private FileDescriptor fd;
	private final long size;
	private long bytesRead;
	
	private final Type kvType;
	private IKType key;
	private IVType value;
	
	public DFSKeyValueRecordReader(SystemCallInterface sysCall, String path) throws IOException, KeeperException, InterruptedException {
		this.dfs = new DFSInterface(sysCall);
		this.fd = this.dfs.open(path, FileMode.READER);
		this.size = this.dfs.size(fd);
		this.bytesRead = 0;
		this.kvType = new TypeToken< SimpleEntry<IKType, IVType> >() {}.getType();
	}

	public IKType currentKey(){
		return this.key;
	}

	public IVType currentValue(){
		return this.value;
	}

	public boolean nextKeyValue() throws IOException, KeeperException, InterruptedException {
		String json = this.dfs.readLine(this.fd);
		if(json==null) return false;
		SimpleEntry<IKType, IVType> kv = Json.loads(json, this.kvType);
		this.key = kv.getKey();
		this.value = kv.getValue();
		this.bytesRead += json.length()+1;
		return true;
	}

	public void close() throws IOException, KeeperException, InterruptedException {
		this.dfs.close(fd);
	}

	public float getProgress(){
		return ((float)this.bytesRead)/((float)this.size);
	}
}

