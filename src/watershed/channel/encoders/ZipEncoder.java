package watershed.channel.encoders;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.zip.*;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import cloudos.util.Logger;

import watershed.core.ChannelEncoder;

public class ZipEncoder extends ChannelEncoder<String,String>{
	public void start(){
		super.start();
	}
	public void finish(){
		super.finish();
	}

	private static byte[] compress(String stringToCompress) throws UnsupportedEncodingException {
		byte[] compressedData = new byte[1024];
		byte[] stringAsBytes = stringToCompress.getBytes("UTF-8");

		Deflater compressor = new Deflater();
		compressor.setInput(stringAsBytes);
		compressor.finish();
		int compressedDataLength = compressor.deflate(compressedData);

		return Arrays.copyOf(compressedData, compressedDataLength);
	}

	public void send(String data){
		try{
			byte[] compressedData  = compress(data);
			String zipData = Base64.encodeBase64String(compressedData);
			getChannelSender().send(zipData);
		}catch(UnsupportedEncodingException e){
			e.printStackTrace();
		}
	}
}
