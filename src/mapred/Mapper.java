package mapred;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import dpm.BasicProcess;
 
public abstract class Mapper<IKType, IVType, OKType, OVType> extends BasicProcess {
	
	private OutputCollector<OKType, OVType> output;
	private RecordReader<IKType, IVType> input;

	public void start(){
		super.start();
		
		String outPath = "/output"; //TODO get output path
		String inPath = "/input"; //TODO get input path
		try{
			output = new DFSOutputCollector<OKType, OVType>(getSystemCallInterface(), outPath);
			input = new DFSKeyValueRecordReader<IKType, IVType>(getSystemCallInterface(), inPath);
			while(input.nextKeyValue()){
				map(input.currentKey(),input.currentValue(),output);
			}
		}catch(Exception e){ //TODO make a better try catch
			e.printStackTrace();
		}
		//TODO send end of work signal to master
	}
	
	public void finish(){
		super.finish();
		try{
			output.close();
			input.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	public abstract void map(IKType key, IVType value, OutputCollector<OKType, OVType> output);
	

}
