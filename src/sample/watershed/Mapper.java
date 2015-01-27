package sample.watershed;

import java.util.AbstractMap.SimpleEntry;

import cloudos.util.Logger;

/*import watershed.core.Filter;
import watershed.core.ControlMessage;
*/
import watershed.core.*;

public class Mapper extends Filter<String, SimpleEntry<String,String>>{
	public void start(){
		super.start();
		//Logger.info("Filter: start");
	}
	public void finish(){
		super.finish();
	}

	public void process(String src, String data){
		Logger.info("mapping: "+data);
		for(String channel: getOutputChannels()){
			String []words = data.split("\\W+");
			for(String word : words){
				getOutputChannel(channel).send(new SimpleEntry<String,String>(word, "1"));
			}
		}
	}
	
	public void onChannelHalt(String channelName){
		//Logger.info("input channel halted: "+channelName);
	}

	public void onChannelsHalted(){
		//Logger.info("all input channels have halted");
		try{
			halt();
			//Logger.info("filter halted: "+getName());
		}catch(Exception e){
			//Logger.info("ERROR while halting filter: "+getName());
		}
	}

}
