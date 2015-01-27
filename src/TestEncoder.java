
import java.io.*;

import cloudos.util.Logger;

import watershed.core.ChannelEncoder;

public class TestEncoder extends ChannelEncoder<String,String>{
	public void start(){
		super.start();
	}
	public void finish(){
		super.finish();
	}
	public void send(String data){
		getChannelSender().send("<*>"+data+"<*>");
	}
}
