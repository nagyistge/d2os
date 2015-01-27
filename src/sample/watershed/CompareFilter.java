package sample.watershed;

import java.util.Map;
//import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.Comparator;

import cloudos.util.Logger;
import cloudos.util.Json;

import watershed.core.Filter;
import watershed.core.ControlMessage;

public class CompareFilter extends Filter<SimpleEntry<Integer, String>, SimpleEntry<String, String>>{
	private int k;
	private List<String> training;
	private List<SimpleEntry<Integer, String>> samples;
	private boolean finishedTraining;

	public void start(){
		super.start();

		this.samples = new ArrayList<SimpleEntry<Integer, String>>();
		this.training = new ArrayList<String>();
		this.k = Integer.parseInt(getAttribute("k"));
		//this.finishedTraining = false;
	}

	public void finish(){
		super.finish();
	}

	public void process(String src, SimpleEntry<Integer, String> data){
		if(this.training==null || this.samples==null){
			//Logger.info("LOSING DATA: instance not initialized yet");
		}else if(data==null || data.getValue()==null || data.getValue().trim().length()==0){
			//Logger.info("EMPTY DATA");
		}else if("Training".equals(src)){
			//Logger.info("process: "+Json.dumps(data));
			this.training.add(data.getValue());
		}else if("Samples".equals(src)){
			//Logger.info("process: "+Json.dumps(data));
			//if(!this.finishedTraining){
				this.samples.add( new SimpleEntry<Integer, String>(data.getKey(), data.getValue()) );
			/*}else{
				//Logger.info("Classifying sample:");
				SimpleEntry<Integer, String> outData = new SimpleEntry<Integer, String>(data.getKey(), classify(data.getValue()));
				send(outData);
			}*/
		}
	}

	public void onChannelHalt(String channelName){
		//Logger.info("input channel halted: "+channelName);
		/*if("Training".equals(channelName)){
			this.finishedTraining = true;
			//compare samples
			//Logger.info("Classifying samples");
			//Logger.info("Samples: "+this.samples.size());
			for(SimpleEntry<Integer, String> data : this.samples){
				SimpleEntry<Integer, String> outData = new SimpleEntry<Integer, String>(data.getKey(), classify(data.getValue()));
				send(outData);
			}
			this.samples.clear();
		}*/
	}

	public void onChannelsHalted(){
		for(SimpleEntry<Integer, String> data : this.samples){
                       SimpleEntry<Integer, String> outData = new SimpleEntry<Integer, String>(data.getKey(), classify(data.getValue()));
                       send(outData);
                }
                this.samples.clear();
		this.training.clear();
		//Logger.info("all input channels have halted");
		try{
			halt();
			//Logger.info("filter halted: "+getName());
		}catch(Exception e){
			//Logger.info("ERROR while halting filter: "+getName());
		}
	}

	public void send(SimpleEntry<Integer, String> data){
		//String str = Json.dumps(data);
		for(String channel: getOutputChannels()){
			getOutputChannel(channel).send(new SimpleEntry<String, String>(data.getKey().toString(), data.getValue()));
		}
	}

	public String classify(String data){
		//Logger.info("data: "+data);
		String []tuple = data.split(" ");
		List<SimpleEntry<Integer, Double>> neighbors = new ArrayList<SimpleEntry<Integer, Double>>();
		for(String trainData: this.training){
			String []trainTuple = trainData.split(" ");
			double sum = 0;
			for(int i = 0; i<tuple.length; i++){
				sum += Math.pow( Double.parseDouble(tuple[i]) - Double.parseDouble(trainTuple[i]) , 2.0);
			}
			double distance = Math.sqrt(sum);
			int category = Integer.parseInt(trainTuple[trainTuple.length-1]);
			neighbors.add(new SimpleEntry<Integer, Double>(new Integer(category), new Double(distance)));
		}
		
		Collections.sort(neighbors, new Comparator() {
			  public int compare(Object o1, Object o2) {

			      Double x1 = ((SimpleEntry<Integer, Double>)o1).getValue();
			      Double x2 = ((SimpleEntry<Integer, Double>)o2).getValue();

			      return x1.compareTo(x2);
			  }
		 });

		/*
		Map<Integer, Integer> categories = new HashMap<Integer, Integer>();
		for(int ik = 0; ik<k; ik++){
			if(categories.containsKey(neighbors.get(ik).getKey())){
				categories.put(neighbors.get(ik).getKey(), new Integer(1));
			}else{
				categories.put(neighbors.get(ik).getKey(), new Integer(categories.get(neighbors.get(ik).getKey()).intValue() + 1));
			}
		}

		
		int maxCat = -1;
		int maxFreq = -1;
		for(Integer key: categories.keySet()){
			if(categories.get(key).intValue()>maxFreq){
				maxFreq = categories.get(key).intValue();
				maxCat = key.intValue();
			}
		}
		
		return maxCat;
		*/

		List<SimpleEntry<Integer, Double>> nearneihbors = new ArrayList<SimpleEntry<Integer, Double>>();
		for(int ik = 0; ik<k; ik++){
			nearneihbors.add(neighbors.get(ik));
		}
		String strOut = Json.dumps(nearneihbors);
		//Logger.info("data: "+data+" out: "+strOut);
		return strOut;
	}
}
