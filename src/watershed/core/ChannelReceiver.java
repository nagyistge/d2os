package watershed.core;

public interface ChannelReceiver<DataType> {
	public void receive(String src, DataType data);
}
