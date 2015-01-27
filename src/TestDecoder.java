
import java.io.*;

import cloudos.util.Logger;

import watershed.core.ChannelDecoder;

public class TestDecoder extends ChannelDecoder<String,String>{
	public void start(){
		super.start();
	}
	public void finish(){
		super.finish();
	}
	public void receive(String src, String data){
		deliver(src, "<#>"+data+"<#>");
	}
}
