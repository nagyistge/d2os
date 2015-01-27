package sample.watershed;

import java.io.*;

import cloudos.kernel.Global;

import cloudos.util.Logger;

import watershed.core.ChannelSender;

public class TestSender extends ChannelSender<String>{
	private PrintWriter out;
	public void start(){
		super.start();
		String fileName = "foo";
		if(getAttribute("file")!=null){
			fileName = getAttribute("file");
		}
		String path = Global.getLocalNodeInfo().getWorkspace()+fileName+"."+getInstance()+".out";
		Logger.info("Sender: start: "+path);
		try{
			out = new PrintWriter(new BufferedWriter(new FileWriter(path)));
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	public void finish(){
		super.finish();
		out.close();
	}
	public void send(String data){
		out.println(data);
		out.flush();
	}
}
