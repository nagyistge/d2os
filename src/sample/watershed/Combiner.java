package sample.watershed;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.AbstractMap.SimpleEntry;

/*
import watershed.core.Filter;
import watershed.core.ChannelEncoder;
import watershed.core.ControlMessage;
*/
import watershed.core.*;

import cloudos.util.Logger;

public class Combiner extends ChannelEncoder<SimpleEntry<String,String>, SimpleEntry<String,String>>{
	private Map<String, Integer> counts;

	public void start(){
		super.start();
		//Logger.info("Combiner: start");
		counts = new ConcurrentHashMap<String, Integer>();
	}

	public void finish(){
		super.finish();

		for(String key: counts.keySet()){
			getChannelSender().send(new SimpleEntry<String,String>(key, counts.get(key).toString()));
		}
	}

	public void send(SimpleEntry<String,String> data){
		if(counts!=null){
		String word = data.getKey().toLowerCase();
		int val = Integer.parseInt(data.getValue());
		if(counts.containsKey(word)){
			counts.put(word, new Integer(val+counts.get(word).intValue()));
		}else {
			counts.put(word, new Integer(val));
		}
		}
	}
}
