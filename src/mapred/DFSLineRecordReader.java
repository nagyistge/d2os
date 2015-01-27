package mapred;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import dfs.DFSInterface;
import dfs.FileDescriptor;
import dfs.FileMode;

import cloudos.kernel.SystemCallInterface;
import cloudos.util.Json;

public class DFSLineRecordReader implements RecordReader<Long, String>{
	private DFSInterface dfs;
	private FileDescriptor fd;
	private final long size;
	private long bytesRead;
	
	private Long key;
	private String value;
	
	public DFSLineRecordReader(SystemCallInterface sysCall, String path) throws IOException, KeeperException, InterruptedException {
		this.dfs = new DFSInterface(sysCall);
		this.fd = this.dfs.open(path, FileMode.READER);
		this.size = this.dfs.size(fd);
		this.bytesRead = 0;

		this.key = new Long(0);
	}

	public Long currentKey(){
		return this.key;
	}

	public String currentValue(){
		return this.value;
	}

	public boolean nextKeyValue() throws IOException, KeeperException, InterruptedException {
		String line = this.dfs.readLine(this.fd);
		if(line==null) return false;
		this.key = new Long(this.key.longValue()+1);
		this.value = line;
		this.bytesRead += line.length()+1;
		return true;
	}

	public void close() throws IOException, KeeperException, InterruptedException {
		this.dfs.close(fd);
	}

	public float getProgress(){
		return ((float)this.bytesRead)/((float)this.size);
	}
}

