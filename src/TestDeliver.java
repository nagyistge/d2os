
import java.io.*;

import cloudos.util.Logger;

import watershed.core.ChannelDeliver;

public class TestDeliver extends ChannelDeliver<String>{
	public void start(){
		super.start();

		Logger.info("Deliver: BEGIN");
		for(int i = 0; i<100; i++)
			deliver("temp", ""+i);
		Logger.info("Deliver: END");
	}
	public void finish(){
		super.finish();
	}
}
