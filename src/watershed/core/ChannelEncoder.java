package watershed.core;

public abstract class ChannelEncoder<InputType, OutputType> extends ChannelSender<InputType> {
	private ChannelSender<OutputType> sender;

	public ChannelSender<OutputType> getChannelSender(){
		return this.sender;
	}

	public void setChannelSender(ChannelSender<OutputType> sender){
		this.sender = sender;
	}
}
