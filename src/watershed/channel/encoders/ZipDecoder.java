package watershed.channel.encoders;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.zip.*;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

import cloudos.util.Logger;

import watershed.core.ChannelDecoder;

public class ZipDecoder extends ChannelDecoder<String,String>{
	public void start(){
		super.start();
	}
	public void finish(){
		super.finish();
	}

	public static String decompressToString(byte[] compressedData) throws UnsupportedEncodingException, DataFormatException {   
		Inflater deCompressor = new Inflater();
		deCompressor.setInput(compressedData, 0, compressedData.length);
		byte[] output = new byte[1024];
		int decompressedDataLength = deCompressor.inflate(output);
		deCompressor.end();

		return new String(output, 0, decompressedDataLength, "UTF-8");
	}

	public void receive(String src, String data){
		try{
			byte []compressedData = Base64.decodeBase64(data);
			String deCompressedString = decompressToString(compressedData);
			deliver(src, deCompressedString);
		}catch(UnsupportedEncodingException e){
			e.printStackTrace();
		}catch(DataFormatException e){
			e.printStackTrace();
		}
	}
}
