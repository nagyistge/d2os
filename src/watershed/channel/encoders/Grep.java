package watershed.channel.encoders;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import watershed.core.Filter;
import watershed.core.ChannelEncoder;
import watershed.core.ControlMessage;

import cloudos.util.Logger;

public class Grep extends ChannelEncoder<String, String>{
	
	private boolean fullString;
	private String patternStr;
	private Pattern pattern;

	public void start(){
		super.start();
		Logger.info("Grep: start");
		
		fullString = !("false".equals(getAttribute("fullstring")));
		patternStr = getAttribute("pattern"); //regular expression

		pattern = Pattern.compile(patternStr);
	}

	public void finish(){
		super.finish();
		Logger.info("Grep: finish");
	}

	public void send(String data){
		Matcher matcher = pattern.matcher(data);
		if(matcher.find()){
			if(fullString){
				getChannelSender().send(data);
			}else{
				do{
					getChannelSender().send(matcher.group());
				}while(matcher.find());
			}
		}
	}
}
