package sample.watershed;

import java.util.Map;
//import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.Comparator;

import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;

import cloudos.util.Logger;
import cloudos.util.Json;

import watershed.core.Filter;
import watershed.core.ControlMessage;

public class MergeFilter extends Filter<SimpleEntry<String, String>, String>{
	private int k;
	Map<String, List<SimpleEntry<Integer, Double>>> candidates;
	public void start(){
		super.start();
		this.k = Integer.parseInt(getAttribute("k"));
		candidates = new ConcurrentHashMap<String, List<SimpleEntry<Integer, Double>>>();
	}

	public void finish(){
		super.finish();
		
		//Logger.info("FINISHING: ");
		//Logger.info("Candidates: "+candidates.keySet().size());
		for(String itemId: candidates.keySet()){
			List<SimpleEntry<Integer, Double>> neighbors = candidates.get(itemId);

			//Logger.info("neighbors: "+neighbors.size());
			Collections.sort(neighbors, new Comparator() {
				  public int compare(Object o1, Object o2) {

					   Double x1 = ((SimpleEntry<Integer, Double>)o1).getValue();
					   Double x2 = ((SimpleEntry<Integer, Double>)o2).getValue();

					   return x1.compareTo(x2);
				  }
			});

			//Logger.info("neighbors: "+neighbors.size()+" k: "+this.k);

			Map<Integer, Integer> categories = new ConcurrentHashMap<Integer, Integer>();
			int ik = 0;
			for(SimpleEntry<Integer, Double> pair: neighbors){
				if(ik>=this.k) break;
				ik++;
				//Logger.info("ik: "+ik+" pair: "+Json.dumps(pair));

				Integer nKey = pair.getKey();
				if(!categories.containsKey(nKey)){
					categories.put(nKey, new Integer(1));
				}else{
					categories.put(nKey, new Integer(categories.get(nKey).intValue() + 1));
				}
			}
			/*
			for(int ik = 0; ik<k; ik++){
				Integer nKey = neighbors.get(ik).getKey();
				if(!categories.containsKey(nKey)){
					categories.put(nKey, new Integer(1));
				}else{
					categories.put(nKey, new Integer(categories.get(nKey).intValue() + 1));
				}
			}
			*/
			//Logger.info("categories: "+Json.dumps(categories));
		
			int maxCat = -1;
			int maxFreq = -1;
			for(Integer key: categories.keySet()){
				if(categories.get(key).intValue()>maxFreq){
					maxFreq = categories.get(key).intValue();
					maxCat = key.intValue();
				}
			}

			send(itemId.toString()+" "+maxCat);
		}
	}

	public void process(String src, SimpleEntry<String, String> data){
		if(data!=null && data.getValue()!=null){
			List<SimpleEntry<Integer, Double>> nearneihbors = Json.loads(data.getValue(), new TypeToken< List<SimpleEntry<Integer, Double>> >() {}.getType());
			//Logger.info("processing: "+data.getKey()+": "+data.getValue());
			if(!candidates.containsKey(data.getKey())){
				candidates.put(data.getKey(), nearneihbors);
			}else{
				candidates.get(data.getKey()).addAll(nearneihbors);
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

	public void send(String data){
		for(String channel: getOutputChannels()){
			getOutputChannel(channel).send(data);
		}
	}
}
