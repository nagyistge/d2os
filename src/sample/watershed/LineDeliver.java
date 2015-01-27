package sample.watershed;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import cloudos.kernel.Global;

import cloudos.kernel.SystemCallInterface;
import cloudos.kernel.ModuleControllerInterface;

import cloudos.util.Logger;

import dfs.DFSInterface;
import dfs.FileDescriptor;
import dfs.FileMode;

import watershed.core.ChannelDeliver;

public class LineDeliver extends ChannelDeliver<String>{
	private DFSInterface dfs;

	private long totalBytesRead;
	
	private String readLine(FileDescriptor fd) throws IOException, KeeperException, InterruptedException{
		StringBuffer str = new StringBuffer();
		int ch = this.dfs.read(fd);
		totalBytesRead++;
		while(ch>0 && ch!='\n'){
			str.append((char)ch);
			ch = this.dfs.read(fd);
			totalBytesRead++;
		}
		if(ch<0 && str.length()==0) return null;
		else return str.toString();
	}
	
	public void start(){
		super.start();

		//partition or not the file among the instances
		boolean partition = !("false".equals(getAttribute("partition")));

		this.totalBytesRead = 0;
		int idx = getInstance();
		int n = getNumInstances();
		long bytesToRead = 0;
		Logger.info("IDX: "+idx);
		Logger.info("N: "+n);
		Logger.info("READING FILE: "+getAttribute("file"));
		this.dfs = new DFSInterface(getSystemCallInterface());

		try{
			//FileDescriptor fd = this.dfs.open("/foo/bar", FileMode.READER);
			FileDescriptor fd = this.dfs.open(getAttribute("file"), FileMode.READER);
			long size = this.dfs.size(fd);
			bytesToRead = (size/((long)n));
			Logger.info("READING: "+bytesToRead+" bytes");
			if(idx>0 && partition){
				long pos = ((long)idx)*bytesToRead;
				Logger.info("START READING AT: "+pos);
				this.dfs.seek(fd, pos-1);
				String temp = readLine(fd);
				Logger.info("PAD LINE: "+temp);
				this.totalBytesRead = 0;
			}
			String line;
			while( (line=readLine(fd))!=null ){
				//out.println(line);
				deliver(getChannelName(), line);
				if(this.totalBytesRead>=bytesToRead && partition) break;
			}
			this.dfs.close(fd);
		}catch(IOException e){
			e.printStackTrace();
		}catch(KeeperException e){
			e.printStackTrace();
		}catch(InterruptedException e){
			e.printStackTrace();
		}
		halt();
	}
	public void finish(){
		super.finish();
	}
}
