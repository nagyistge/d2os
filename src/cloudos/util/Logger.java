package cloudos.util;
/**
 * @author Rodrigo Caetano O. ROCHA
 * @date 25 July 2013
 */

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Logger {
	private final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss:S");
	private static StringBuffer sBuffer = new StringBuffer();

	private static final Lock lock = new ReentrantLock();

	// Suppress default constructor for noninstantiability
	private Logger() {
		throw new AssertionError();
	}

	private static void output(){
		try{ lock.lock();
			System.out.print(sBuffer.toString());
			//sBuffer.setLength(0);
			//sBuffer.delete(0, sBuffer.length());
			sBuffer = new StringBuffer();
		}finally{
			lock.unlock();
		}
	}
	
	public static void info(String message){
		printlog("INFO: "+message);
	}
	
	public static void warning(String message){
		printlog("WARNING: "+message);
	}

	public static void severe(String message){
		printlog("SEVERE: "+message);
	}

	public static void fatal(String message){
		printlog("FATAL: "+message);
	}

	public static void log(String message){
		printlog(message);
	}

	private static void printlog(String message){
		sBuffer.append(dateFormat.format(new Date()));
		//sBuffer.append(dateFormat.format(Calendar.getInstance().getTime()));

		StackTraceElement[] trace = Thread.currentThread().getStackTrace();
		if(trace.length>=3){
			sBuffer.append(' ');
			sBuffer.append(trace[3].getClassName());
			sBuffer.append(' ');
			sBuffer.append(trace[3].getMethodName());
		}
		sBuffer.append('\n');
		sBuffer.append(message);
		sBuffer.append('\n');

		output();
	}
}
