
import cloudos.util.Logger;

import watershed.core.Filter;
import watershed.core.ControlMessage;

public class TestFilter extends Filter<String, String>{
	public void start(){
		super.start();
		Logger.info("Filter: start");
	}
	public void finish(){
		super.finish();
	}

	public void process(String src, String data){
		for(String channel: getOutputChannels()){
			getOutputChannel(channel).send(data);
		}
	}

	public void onChannelHalt(String channelName){
		Logger.info("input channel halted: "+channelName);
	}

	public void onChannelsHalted(){
		Logger.info("all input channels have halted");
		try{
			halt();
			Logger.info("filter halted: "+getName());
		}catch(Exception e){
			Logger.info("ERROR while halting filter: "+getName());
		}
	}

}
