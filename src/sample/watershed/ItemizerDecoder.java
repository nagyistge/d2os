package sample.watershed;

import java.util.AbstractMap.SimpleEntry;

import cloudos.util.Logger;

import watershed.core.ChannelDecoder;

public class ItemizerDecoder extends ChannelDecoder<String, SimpleEntry<Integer, String>>{
	private int id;
	public void start(){
		super.start();
		id = 0;
	}
	public void finish(){
		super.finish();
	}

	public void receive(String src, String data){
		deliver(src, new SimpleEntry<Integer, String>(new Integer(id), data));
		id++;
	}
}
