package mapred;

import java.io.IOException;

import java.util.AbstractMap.SimpleEntry;

import org.apache.zookeeper.KeeperException;

import dfs.DFSInterface;
import dfs.FileDescriptor;
import dfs.FileMode;

import cloudos.kernel.SystemCallInterface;
import cloudos.util.Json;

public class DFSOutputCollector<KType, VType> implements OutputCollector<KType, VType>{
	private DFSInterface dfs;
	private FileDescriptor fd;

	public DFSOutputCollector(SystemCallInterface sysCall, String path) throws IOException{
		this.dfs = new DFSInterface(sysCall);
		try{
			this.fd = this.dfs.open(path, FileMode.WRITER);
		}catch(KeeperException e){
			throw new IOException(e.getMessage());
		}catch(InterruptedException e){
			throw new IOException(e.getMessage());
		}
	}

	public void collect(KType key, VType value) throws IOException {
		String str = Json.dumps(new SimpleEntry<KType, VType>(key, value));
		try{
			this.dfs.write(fd, str);
		}catch(KeeperException e){
			throw new IOException(e.getMessage());
		}catch(InterruptedException e){
			throw new IOException(e.getMessage());
		}
	}

	public void close() throws IOException {
		this.dfs.flush(fd);
		this.dfs.close(fd);
	}
}
