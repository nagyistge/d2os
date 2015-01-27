package watershed.core.info;

import java.util.Deque;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;

public class ChannelInfo {
	private String name;
	private StubInfo senderInfo;
	private Deque<StubInfo> encoderInfoStack;
	private Deque<StubInfo> decoderInfoStack;
	private StubInfo deliverInfo;

	public ChannelInfo(String name){
		this.name = name;
		this.senderInfo = null;
		this.encoderInfoStack = new ArrayDeque<StubInfo>();
		this.decoderInfoStack = new ArrayDeque<StubInfo>();
		this.deliverInfo = null;
	}

	public void setName(String name){
		this.name = name;
	}

	public String getName(){
		return this.name;
	}
	
	public void setSenderInfo(StubInfo senderInfo){
		this.senderInfo = senderInfo;
	}

	public StubInfo getSenderInfo(){
		return this.senderInfo;
	}

	public void setDeliverInfo(StubInfo deliverInfo){
		this.deliverInfo = deliverInfo;
	}

	public StubInfo getDeliverInfo(){
		return this.deliverInfo;
	}

	public Deque<StubInfo> getEncoderInfoStack(){
		return this.encoderInfoStack;
	}

	public Deque<StubInfo> getDecoderInfoStack(){
		return this.decoderInfoStack;
	}

	public void pushEncoderInfo(StubInfo encoderInfo){
		this.encoderInfoStack.push(encoderInfo);
	}

	public StubInfo popEncoderInfo(){
		try{
			return this.encoderInfoStack.pop();
		}catch(NoSuchElementException e){
			return null;
		}
	}

	public void pushDecoderInfo(StubInfo decoderInfo){
		this.decoderInfoStack.push(decoderInfo);
	}

	public StubInfo popDecoderInfo(){
		try{
			return this.decoderInfoStack.pop();
		}catch(NoSuchElementException e){
			return null;
		}
	}
}

