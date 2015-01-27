
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.zip.*;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

public class Temp {

	private static byte[] compress(String stringToCompress) throws UnsupportedEncodingException {
		byte[] compressedData = new byte[1024];
		byte[] stringAsBytes = stringToCompress.getBytes("UTF-8");

		Deflater compressor = new Deflater();
		compressor.setInput(stringAsBytes);
		compressor.finish();
		int compressedDataLength = compressor.deflate(compressedData);

		return Arrays.copyOf(compressedData, compressedDataLength);
	}

	private static String decompressToString(byte[] compressedData) throws UnsupportedEncodingException, DataFormatException {   
		Inflater deCompressor = new Inflater();
		deCompressor.setInput(compressedData, 0, compressedData.length);
		byte[] output = new byte[1024];
		int decompressedDataLength = deCompressor.inflate(output);
		deCompressor.end();

		return new String(output, 0, decompressedDataLength, "UTF-8");
	}

	public static void main(String []args) throws UnsupportedEncodingException, DataFormatException{
		String strToBeCompressed  = "1:9 And God said, Let the waters under the heaven be gathered together\nunto one place, and let the dry land appear: and it was so.\n\n1:10 And God called the dry land Earth; and the gathering together of\nthe waters called he Seas: and God saw that it was good.\n\n1:11 And God said, Let the earth bring forth grass, the herb yielding\nseed, and the fruit tree yielding fruit after his kind, whose seed is\nin itself, upon the earth: and it was so.";
		byte[] compressedData  = compress(strToBeCompressed);
		String deCompressedString = decompressToString(compressedData);

		System.out.println("Original     :: " + strToBeCompressed.length() + " " + strToBeCompressed);
		System.out.println("Compressed   :: " + compressedData.length + " " + compressedData.toString());
		String str = Base64.encodeBase64String(compressedData);
		System.out.println("CompressedB64   :: " + str.length() + " " + str);
		System.out.println("decompressed :: " + deCompressedString.length() + " " + deCompressedString);
	}
}
