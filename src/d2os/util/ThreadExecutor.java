package d2os.util;

import java.util.Map;
import java.util.HashMap;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ThreadExecutor<KeyType, HandlerType extends Thread> {
	private Map<KeyType, HandlerType> handlers;
	private ExecutorService serverExecutor;

	public ThreadExecutor(){
		handlers = new HashMap<KeyType, HandlerType>();
		serverExecutor = Executors.newCachedThreadPool();
	}

	public void addThread(KeyType key, HandlerType handler){
		if(!handlers.keySet().contains(key)){
			handlers.put(key, handler);
			serverExecutor.execute(handler);
		}else throw new IllegalArgumentException("Duplicated key: "+key+" already exists.");
	}

	public void removeThread(KeyType key){
		HandlerType handler = handlers.remove(key);
		if(handler!=null){
			handler.interrupt();
		}
	}

	public HandlerType getThread(String key){
		return handlers.get(key);
	}

	//TODO implement a interrupt(), stopAll(), or removeTheads() function
}
