package watershed.core.info;

import java.util.Deque;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;

public class SenderLoadInfo {
	private String channelName;
	private String srcFilterName;

	private String dstFilterName;
	private int dstInstances;

	private StubInfo senderInfo;
	private Deque<StubInfo> encoderInfoStack;

	public SenderLoadInfo(String channelName, String srcFilterName, String dstFilterName, int dstInstances){
		this.channelName = channelName;
		this.srcFilterName = srcFilterName;
		this.dstFilterName = dstFilterName;
		this.dstInstances = dstInstances;
	}

	public String getChannelName(){
		return this.channelName;
	}

	public void setChannelName(String channelName){
		this.channelName = channelName;
	}

	public String getSourceFilterName(){
		return this.srcFilterName;
	}

	public void setSourceFilterName(String srcFilterName){
		this.srcFilterName = srcFilterName;
	}

	public String getDestinationFilterName(){
		return this.dstFilterName;
	}

	public void setDestinationFilterName(String dstFilterName){
		this.dstFilterName = dstFilterName;
	}

	public int getDestinationInstances(){
		return this.dstInstances;
	}

	public void setDestinationInstances(int dstInstances){
		this.dstInstances = dstInstances;
	}

	public StubInfo getSenderInfo(){
		return this.senderInfo;
	}

	public void setSenderInfo(StubInfo senderInfo){
		this.senderInfo = senderInfo;
	}

	public void setEncoderInfoStack(Deque<StubInfo> encoderInfoStack){
		this.encoderInfoStack = encoderInfoStack;
	}

	public StubInfo popEncoderInfo(){
		try{
			return this.encoderInfoStack.pop();
		}catch(NoSuchElementException e){
			return null;
		}
	}
}
