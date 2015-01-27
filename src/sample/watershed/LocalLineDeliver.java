package sample.watershed;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.BufferedInputStream;

import org.apache.zookeeper.KeeperException;

import cloudos.kernel.Global;

import cloudos.kernel.SystemCallInterface;
import cloudos.kernel.ModuleControllerInterface;

import cloudos.util.Logger;

import watershed.core.ChannelDeliver;

public class LocalLineDeliver extends ChannelDeliver<String>{
	//private RandomAccessFile reader;
	private BufferedInputStream reader;
	private long totalBytesRead;
	
	//private String readLine(RandomAccessFile reader) throws IOException {
	private String readLine(BufferedInputStream reader) throws IOException {
		StringBuffer str = new StringBuffer();
		int ch = reader.read();
		totalBytesRead++;
		while(ch>0 && ch!='\n'){
			str.append((char)ch);
			ch = reader.read();
			totalBytesRead++;
		}
		if(ch<0 && str.length()==0) return null;
		else return str.toString();
	}
	
	public void start(){
		super.start();

		boolean partition = !("false".equals(getAttribute("partition")));

		this.totalBytesRead = 0;
		int idx = getInstance();
		int n = getNumInstances();
		long bytesToRead = 0;
		Logger.info("IDX: "+idx);
		Logger.info("N: "+n);
		Logger.info("READING FILE: "+getAttribute("file"));

		try{
			File file = new File(getAttribute("file"));
			//reader = new RandomAccessFile(file, "r");
			long size = file.length();
			bytesToRead = (size/((long)n));
			Logger.info("READING: "+bytesToRead+" bytes");
			if(idx>0 && partition){
				long pos = ((long)idx)*bytesToRead;
				Logger.info("START READING AT: "+pos);
				RandomAccessFile raf = new RandomAccessFile(file, "r");
				raf.seek(pos-1);
				reader = new BufferedInputStream(new FileInputStream(raf.getFD()));
				String temp = readLine(reader);
				Logger.info("PAD LINE: "+temp);
				this.totalBytesRead = 0;
			}else {
				reader = new BufferedInputStream(new FileInputStream(file));
			}
			String line;
			while( (line=readLine(reader))!=null){
				//out.println(line);
				deliver(getChannelName(), line);
				if(this.totalBytesRead>=bytesToRead && partition) break;
			}
			reader.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		halt();
	}
	public void finish(){
		super.finish();
	}
}
