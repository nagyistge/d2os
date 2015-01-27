package sample.watershed;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.AbstractMap.SimpleEntry;

import cloudos.util.Logger;
/*
import watershed.core.Filter;
import watershed.core.ControlMessage;
*/
import watershed.core.*;

public class Reducer extends Filter<SimpleEntry<String,String>, String>{
	private Map<String, Integer> counts;
	public void start(){
		super.start();
		//Logger.info("Filter: start");
		counts = new ConcurrentHashMap<String, Integer>();
	}

	public void finish(){
		super.finish();
		Logger.info("REDUCER FINISHING: "+counts.keySet().size());
		for(String key: counts.keySet()){
			for(String channel: getOutputChannels()){
				getOutputChannel(channel).send(key+"="+counts.get(key).toString());
			}
		}
		Logger.info("REDUCER FINISHED");
	}

	public void process(String src, SimpleEntry<String,String> data){
		Logger.info("reducing: "+data.toString());
		String word = data.getKey().toLowerCase();
		int val = Integer.parseInt(data.getValue());
		if(counts.containsKey(word)){
			counts.put(word, new Integer(val+counts.get(word).intValue()));
		}else {
			counts.put(word, new Integer(val));
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
			//Logger.info("ERROR while halting filter: "+getName());
		}
	}

}
