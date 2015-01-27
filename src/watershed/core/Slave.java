package watershed.core;

import java.io.IOException;

import java.util.Deque;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;

import java.util.AbstractMap.SimpleEntry;

import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


import java.net.MalformedURLException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;

import cloudos.kernel.ModuleSlave;
import cloudos.kernel.info.ExecutorInfo;
//import cloudos.kernel.ExecutionController;
import cloudos.kernel.DefaultExecutor;
import cloudos.kernel.SystemCallRequest;
import cloudos.kernel.SystemCallReply;
import cloudos.kernel.SystemCallErrorType;
import cloudos.kernel.Global;

import cloudos.util.Json;
import cloudos.util.Logger;
import cloudos.util.ClassUtil;

import watershed.core.info.ModuleInfo;
import watershed.core.info.ModuleLoadInfo;
import watershed.core.info.SenderLoadInfo;
import watershed.core.info.FilterInfo;
import watershed.core.info.ChannelInfo;
import watershed.core.info.StubInfo;

class ExecutorThread<ExecutorType extends DefaultExecutor>  extends Thread {
	private ExecutorType executor;

	public ExecutorThread(ExecutorType executor){
		this.executor = executor;
	}

	public void run(){
		this.executor.start();
		//this.executor.finish();
	}

	public ExecutorType getExecutor(){
		return this.executor;
	}
}

public class Slave extends ModuleSlave{
	private final Lock lock = new ReentrantLock();

	private Map<String, Map<Long, ExecutorThread<ChannelDeliver>>> executors;
	private Map<String, Map<Integer, Filter>> filters;
	private ExecutorService serverExecutor;

	public void start(){
		super.start();
		this.executors = new ConcurrentHashMap<String, Map<Long, ExecutorThread<ChannelDeliver>>>();
		this.filters = new ConcurrentHashMap<String, Map<Integer, Filter>>();
		this.serverExecutor = Executors.newCachedThreadPool();
	}
	
	public void finish(){
		super.finish();
	}

	private String getPath(String filterName){
		String path = Global.getLocalNodeInfo().getWorkspace();
		path += "modules/watershed/"+filterName+"/"+RandomStringUtils.randomAlphanumeric(16)+".jar";
		return path;
	}

	public void startDelivers(ChannelDeliver deliver){
		while(deliver!=null){
			if(!deliver.isRunning()){
				deliver.start();
			}
			ChannelReceiver receiver = deliver.getChannelReceiver();
			if(receiver instanceof ChannelDeliver){
				deliver = (ChannelDeliver)receiver;
			}else deliver = null;
		}
	}
	
	public void startSenders(ChannelSender sender){
		while(sender!=null){
			if(!sender.isRunning()){
				sender.start();
			}
			if(sender instanceof ChannelEncoder){
				sender = ((ChannelEncoder)sender).getChannelSender();
			}else sender = null;
		}
	}

	private void loadDelivers(ModuleLoadInfo loadInfo, Filter filter){
		String path;

		for(String channelName: loadInfo.getModuleInfo().getInputChannelInfo().keySet()){
			ChannelInfo channelInfo = loadInfo.getModuleInfo().getInputChannelInfo(channelName);

			StubInfo stubInfo = null;
			ChannelReceiver receiver = filter;
			while( (stubInfo = channelInfo.popDecoderInfo())!=null ){
				path = null;
				if(stubInfo.getFileBase64()!=null){
					path = getPath(loadInfo.getModuleInfo().getFilterInfo().getName());
					try{
						stubInfo.writeFile(path);
					}catch(IOException e){
						e.printStackTrace();
					}
				}

				ChannelDecoder decoder = null;
				try{
					decoder = (ChannelDecoder)ClassUtil.load(stubInfo.getClassName(), path).newInstance();
					decoder.setAttributes(stubInfo.getAttributes());
				}catch(MalformedURLException e){
					e.printStackTrace();
				}catch(ClassNotFoundException e){
					e.printStackTrace();
				}catch(InstantiationException e){
					e.printStackTrace();
				}catch(IllegalAccessException e){
					e.printStackTrace();
				}
				decoder.setSystemCallInterface(getSystemCallInterface());
				decoder.setChannelReceiver(receiver);
				decoder.setInstance(loadInfo.getInstance());
				decoder.setNumInstances(loadInfo.getModuleInfo().getInstances());
				decoder.start();
				receiver = decoder;
			}
			stubInfo = channelInfo.getDeliverInfo();

			path = null;
			if(stubInfo.getFileBase64()!=null){
				path = getPath(loadInfo.getModuleInfo().getFilterInfo().getName());
				try{
					stubInfo.writeFile(path);
				}catch(IOException e){
					e.printStackTrace();
				}
			}

			ChannelDeliver deliver = null;
			try{
				deliver = (ChannelDeliver)ClassUtil.load(stubInfo.getClassName(), path).newInstance();
				deliver.setAttributes(stubInfo.getAttributes());
			}catch(MalformedURLException e){
				e.printStackTrace();
			}catch(ClassNotFoundException e){
				e.printStackTrace();
			}catch(InstantiationException e){
				e.printStackTrace();
			}catch(IllegalAccessException e){
				e.printStackTrace();
			}
			deliver.setSystemCallInterface(getSystemCallInterface());
			deliver.setChannelReceiver(receiver);	
			deliver.setInstance(loadInfo.getInstance());
			deliver.setNumInstances(loadInfo.getModuleInfo().getInstances());
			deliver.setChannelName(channelName);
			deliver.setFilter(filter);

			ExecutorThread<ChannelDeliver> handler = new ExecutorThread<ChannelDeliver>(deliver);
			this.executors.get(loadInfo.getModuleInfo().getFilterInfo().getName()).put( new Long(handler.getId()), handler );
			this.serverExecutor.execute( handler );
		}
	}

	private void loadSenders(ModuleLoadInfo loadInfo, Filter filter){
		String path;
		//Logger.info("loading SENDERS ok");
		//Logger.info("loading SENDERS: "+loadInfo.getModuleInfo().getOutputChannelInfo().size());
		if(loadInfo.getModuleInfo().getOutputChannelInfo().isEmpty())return; //TODO load the channel names?

		Logger.info("loading SENDERS names: "+Json.dumps(loadInfo.getModuleInfo().getOutputChannelInfo().keySet()));
		for(String channelName: loadInfo.getModuleInfo().getOutputChannelInfo().keySet()){
			Logger.info("adding SENDER: "+channelName);
			ChannelInfo channelInfo = loadInfo.getModuleInfo().getOutputChannelInfo(channelName);

			if(channelInfo==null) {
				Logger.info("channel SENDER null");
				continue;
			}
			StubInfo stubInfo = null;
			stubInfo = channelInfo.getSenderInfo();
			if(stubInfo==null) {
				Logger.info("stub SENDER null");
				continue;
			}
			path = null;
			if(stubInfo.getFileBase64()!=null){
				path = getPath(loadInfo.getModuleInfo().getFilterInfo().getName());
				try{
					stubInfo.writeFile(path);
				}catch(IOException e){
					e.printStackTrace();
				}
			}

			//Logger.info("loading SENDER class");
			ChannelSender sender = null;
			try{
				sender = (ChannelSender)ClassUtil.load(stubInfo.getClassName(), path).newInstance();
				Logger.info("loaded SENDER class: "+stubInfo.getClassName());
				sender.setSystemCallInterface(getSystemCallInterface());
				sender.setInstance(loadInfo.getInstance());
				sender.setAttributes(stubInfo.getAttributes());
			}catch(MalformedURLException e){
				e.printStackTrace();
			}catch(ClassNotFoundException e){
				e.printStackTrace();
			}catch(InstantiationException e){
				e.printStackTrace();
			}catch(IllegalAccessException e){
				e.printStackTrace();
			}

			Logger.info("loading ENCODERS");
			while( (stubInfo = channelInfo.popEncoderInfo())!=null ){
				path = null;
				if(stubInfo.getFileBase64()!=null){
					path = getPath(loadInfo.getModuleInfo().getFilterInfo().getName());
					try{
						stubInfo.writeFile(path);
					}catch(IOException e){
						e.printStackTrace();
					}
				}

				ChannelEncoder encoder = null;
				try{
					encoder = (ChannelEncoder)ClassUtil.load(stubInfo.getClassName(), path).newInstance();
					encoder.setAttributes(stubInfo.getAttributes());
				}catch(MalformedURLException e){
					e.printStackTrace();
				}catch(ClassNotFoundException e){
					e.printStackTrace();
				}catch(InstantiationException e){
					e.printStackTrace();
				}catch(IllegalAccessException e){
					e.printStackTrace();
				}
				encoder.setSystemCallInterface(getSystemCallInterface());
				encoder.setChannelSender(sender);
				encoder.setInstance(loadInfo.getInstance());
				//sender.start();
				sender = encoder;
			}
			Logger.info("starting final SENDER");
			//sender.start();
			startSenders(sender);
			filter.getOutputChannel(channelName).addChannelSender(loadInfo.getModuleInfo().getFilterInfo().getName(), sender);
		}
		//Logger.info("DONE here");
	}

	private void loadSender(SenderLoadInfo loadInfo, Filter filter){
		Logger.info("Loading sender in: "+loadInfo.getSourceFilterName());
		Logger.info("loading info: "+Json.dumps(loadInfo));

		String path;

		StubInfo stubInfo = null;
		
		stubInfo = loadInfo.getSenderInfo();
		if(stubInfo==null){
			Logger.fatal("Stub is NULL");
		}
		path = null;
		if(stubInfo.getFileBase64()!=null){
			path = getPath(loadInfo.getDestinationFilterName());
			try{
				stubInfo.writeFile(path);
			}catch(IOException e){
				e.printStackTrace();
			}
		}

		Logger.info("loading sender class: "+stubInfo.getClassName());
		ChannelSender sender = null;
		try{
			Logger.info("stub: "+Json.dumps(stubInfo));
			Logger.info("path: "+Json.dumps(path));
			sender = (ChannelSender)ClassUtil.load(stubInfo.getClassName(), path).newInstance();
			//Logger.info("here 1");
			sender.setSystemCallInterface(getSystemCallInterface());
			//Logger.info("here 2");
			sender.setInstance(filter.getInstance());
			
			sender.setDestinationInstances(loadInfo.getDestinationInstances());
			sender.setDestinationFilterName(loadInfo.getDestinationFilterName());
			sender.setSourceFilterName(loadInfo.getSourceFilterName());
			sender.setChannelName(loadInfo.getChannelName());
			sender.setAttributes(stubInfo.getAttributes());
			//Logger.info("here 3");
		}catch(MalformedURLException e){
			e.printStackTrace();
		}catch(ClassNotFoundException e){
			e.printStackTrace();
		}catch(InstantiationException e){
			e.printStackTrace();
		}catch(IllegalAccessException e){
			e.printStackTrace();
		}

		Logger.info("loading encoders");
		while( (stubInfo = loadInfo.popEncoderInfo())!=null ){
			path = null;
			if(stubInfo.getFileBase64()!=null){
				path = getPath(loadInfo.getDestinationFilterName());
				try{
					stubInfo.writeFile(path);
				}catch(IOException e){
					e.printStackTrace();
				}
			}

			ChannelEncoder encoder = null;
			try{
				encoder = (ChannelEncoder)ClassUtil.load(stubInfo.getClassName(), path).newInstance();
				encoder.setAttributes(stubInfo.getAttributes());
			}catch(MalformedURLException e){
				e.printStackTrace();
			}catch(ClassNotFoundException e){
				e.printStackTrace();
			}catch(InstantiationException e){
				e.printStackTrace();
			}catch(IllegalAccessException e){
				e.printStackTrace();
			}
			encoder.setSystemCallInterface(getSystemCallInterface());
			encoder.setChannelSender(sender);
			encoder.setInstance(filter.getInstance());
			//sender.start();
			sender = encoder;
		}
		Logger.info("starting final sender");
		//sender.start();
		//startSenders(sender);
		filter.getOutputChannel(loadInfo.getChannelName()).addChannelSender(loadInfo.getDestinationFilterName(), sender);
		Logger.info("DONE! here");
		startSenders(sender);
	}

	private Filter loadFilter(ModuleLoadInfo loadInfo){
		String path = null;
		Logger.info("BEGIN");
		if(loadInfo.getModuleInfo().getFilterInfo().getFileBase64()!=null){
			path = getPath(loadInfo.getModuleInfo().getFilterInfo().getName());
			try{
				loadInfo.getModuleInfo().getFilterInfo().writeFile(path);
			}catch(IOException e){
				e.printStackTrace();
			}
		}

		Logger.info("loading class Filter");
		Filter filter = null;
		try{
			filter = (Filter)ClassUtil.load(loadInfo.getModuleInfo().getFilterInfo().getClassName(), path).newInstance();
		Logger.info("DONE! loading class: "+loadInfo.getModuleInfo().getFilterInfo().getClassName());
		}catch(MalformedURLException e){
			e.printStackTrace();
		}catch(ClassNotFoundException e){
			e.printStackTrace();
		}catch(InstantiationException e){
			e.printStackTrace();
		}catch(IllegalAccessException e){
			e.printStackTrace();
		}
		filter.setSystemCallInterface(getSystemCallInterface());
		Logger.info("setName");
		filter.setName(loadInfo.getModuleInfo().getFilterInfo().getName());
		Logger.info("setInstance");
		filter.setInstance(loadInfo.getInstance());
		Logger.info("setAttributes");
		filter.setAttributes(loadInfo.getModuleInfo().getFilterInfo().getAttributes());
		Logger.info("setInputChannels");
		if(loadInfo.getModuleInfo().getInputChannelInfo().size()>0){
			filter.setInputChannels(loadInfo.getModuleInfo().getInputChannelInfo().keySet());
		}
		Logger.info("setOutputChannels");
		if(loadInfo.getModuleInfo().getOutputChannelInfo().size()>0){
			filter.setOutputChannels(loadInfo.getModuleInfo().getOutputChannelInfo().keySet());
		}
		Logger.info("start");
		//filter.start();
		return filter;
	}
	
	private void loadModule(ModuleLoadInfo loadInfo){
		Logger.info("loading module: "+loadInfo.getModuleInfo().getFilterInfo().getName());
		if(!this.executors.keySet().contains(loadInfo.getModuleInfo().getFilterInfo().getName())){
			//this.executors.put(loadInfo.getModuleInfo().getFilterInfo().getName(), new ArrayList<ExecutorThread<ChannelDeliver>>());
			this.executors.put(loadInfo.getModuleInfo().getFilterInfo().getName(), new ConcurrentHashMap<Long, ExecutorThread<ChannelDeliver>>());
			this.filters.put(loadInfo.getModuleInfo().getFilterInfo().getName(), new ConcurrentHashMap<Integer, Filter>());
		}
		Logger.info("loading filter");
		Filter filter = loadFilter(loadInfo);
		Logger.info("loading senders");
		loadSenders(loadInfo, filter);
		this.filters.get(loadInfo.getModuleInfo().getFilterInfo().getName()).put(new Integer(loadInfo.getInstance()), filter);
		Logger.info("loading delivers");
		loadDelivers(loadInfo, filter);
		Logger.info("DONE! loading module: "+loadInfo.getModuleInfo().getFilterInfo().getName());
		
		filter.start();
		
	}

	private void loadSender(SenderLoadInfo loadInfo){
		Logger.info("Loading sender in: "+loadInfo.getSourceFilterName());
		for(Integer instance: this.filters.get(loadInfo.getSourceFilterName()).keySet()){
			Filter filter = this.filters.get(loadInfo.getSourceFilterName()).get(instance);
			if(filter.getOutputChannels().contains(loadInfo.getChannelName())){
				loadSender(loadInfo, filter);
			}
		}
	}

	private void sendCtrlMsg(ControlMessage ctrlMsg){
		Logger.info("sending ctrl msg: "+Json.dumps(ctrlMsg));
		int instance = -1;
		String dst = ctrlMsg.getDestination();
		if(dst.indexOf(':')>=0){
			instance = Integer.parseInt(dst.substring(dst.indexOf(':')+1));
			dst = dst.substring(0,dst.indexOf(':'));
		}
		Deque<String> path = ctrlMsg.parsePath(dst);
		if(path==null) return;
		String filterName = path.pop();
		String type = path.pop();
		if("in".equals(type)){
			String portName = path.pop();
			//try{
			//lock.lock();
			Logger.info("LOCK");

			Map<Long, ExecutorThread<ChannelDeliver>> filterDst = executors.get(filterName);
			//for(ExecutorThread<ChannelDeliver> thread: filterDst){
			Set<Long> keys = new HashSet<Long>(filterDst.keySet());
			for(Long exKey : keys){
				ExecutorThread<ChannelDeliver> thread = filterDst.get(exKey);
				if(thread!=null){
					ChannelDeliver deliverExecutor = thread.getExecutor();
					if(deliverExecutor.getChannelName().equals(portName) && (deliverExecutor.getInstance()==instance || instance<0)){
						if(deliverExecutor instanceof ControlMessageReceiver){
							ControlMessageReceiver msgRec = (ControlMessageReceiver)deliverExecutor;
							msgRec.receiveCtrlMsg(ctrlMsg);
						}
					}
				}
			}
			
			//}catch(Exception e){
			//	e.printStackTrace();
			//}finally{
			//	lock.unlock();
				Logger.info("UNLOCK");
			//}
		}else if("out".equals(type)){
			String portName = path.pop();

			Map<Integer, Filter> filterDst = this.filters.get(filterName);
			for(Integer key: filterDst.keySet()){
				Filter filter = filterDst.get(key);
				if(filter.getInstance()==instance || instance<0){
					ChannelOutputSet<?> chanSet = filter.getOutputChannel(portName);
					ChannelSender chanSend = chanSet.getChannelSender(path.pop());
					while(chanSend instanceof ChannelEncoder){
						chanSend = ((ChannelEncoder)chanSend).getChannelSender();
					}
					if(chanSend instanceof ControlMessageReceiver){
						ControlMessageReceiver msgRec = (ControlMessageReceiver)chanSend;
						msgRec.receiveCtrlMsg(ctrlMsg);
					}
				}
			}	
		}
	}

	public void removeFilterInstance(SimpleEntry<String, Integer> pair) {
		Logger.info("removing filter instance: "+Json.dumps(pair));
		String filterName = pair.getKey();
		int instance = pair.getValue().intValue();
		
		//try{
		//lock.lock();
		Logger.info("LOCK");

		Map<Long, ExecutorThread<ChannelDeliver>> deliverThreads = executors.get(filterName);
		//Iterator<ExecutorThread<ChannelDeliver>> iter = deliverThreads.iterator();
		//for(ExecutorThread<ChannelDeliver> thread: deliverThreads){
		Logger.info("Finishing channel delivers");
		//while(iter.hasNext()){
		List<Long> removed = new ArrayList<Long>();
		for(Long exKey : deliverThreads.keySet()){
                        ExecutorThread<ChannelDeliver> thread = deliverThreads.get(exKey);
			//ExecutorThread<ChannelDeliver> thread = iter.next();
			if(thread!=null){
			ChannelDeliver deliver = thread.getExecutor();

			thread.interrupt(); //end thread execution

			if(deliver.getInstance()==instance){
				deliver.finish();
				Logger.info("Finishing deliver: "+deliver.getClass().getSimpleName());
				ChannelReceiver receiver = deliver.getChannelReceiver();
				while(receiver instanceof ChannelDeliver){
					deliver = (ChannelDeliver)receiver;
					Logger.info("Finishing deliver: "+deliver.getClass().getSimpleName());
					deliver.finish();
					receiver = deliver.getChannelReceiver();
				}
				//iter.remove();
				removed.add(exKey);
			}
			}
		}
		
		for(Long exKey: removed){
			deliverThreads.remove(exKey);
			//deliverThreads.put(exKey, null);
		}
		Logger.info("Channel delivers finished");
		//}catch(Exception e){
		//	e.printStackTrace();
		//}finally{
		//	lock.unlock();
			Logger.info("UNLOCK");
		//}
		Map<Integer, Filter> filterInstances = filters.get(filterName);
		if(filterInstances!=null){
			Logger.info("Finishing filter");
			Filter filter = filterInstances.get(new Integer(instance));
			filter.finish();
			Logger.info("Finishing output channels");
			Set<String> outChannels = filter.getOutputChannels();
			for(String chann: outChannels){
				Logger.info("Finishing output channel: "+chann);
				ChannelOutputSet outSet = filter.getOutputChannel(chann);
				Map<String, ChannelSender> chanSenders = outSet.getChannelSenders();
				for(String key: chanSenders.keySet()){
					Logger.info("Finishing sender to: "+key);
					ChannelSender sender = chanSenders.get(key);
					while(sender!=null){
						sender.finish();
						if(sender instanceof ChannelEncoder){
							sender = ((ChannelEncoder)sender).getChannelSender();
						}else sender = null;
					}
				}
				Logger.info("Channels finished: "+chann);
				//outSet.finish();
			}
			Logger.info("Removing instance entry");
			filterInstances.remove(new Integer(instance));
		}
		Logger.info("DONE! removing filter instance");
	}

	public SystemCallReply handleSystemCall(SystemCallRequest sysCallMsg){
		if("loadModule".equals(sysCallMsg.getMethod())){
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null);
			//try{
			//lock.lock();
				ModuleLoadInfo loadInfo = Json.loads(sysCallMsg.getJSONValue(), ModuleLoadInfo.class);
				loadModule(loadInfo);
			/*}catch(Exception e){
				reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null, e.getMessage(), SystemCallErrorType.FATAL);
			}finally{
				lock.unlock();
			}*/
			return reply;
		}else if("loadSender".equals(sysCallMsg.getMethod())){
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null);
			//try{
			//lock.lock();
				SenderLoadInfo loadInfo = Json.loads(sysCallMsg.getJSONValue(), SenderLoadInfo.class);
				loadSender(loadInfo);
			/*}catch(Exception e){
				reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null, e.getMessage(), SystemCallErrorType.FATAL);
			}finally{
				lock.unlock();
			}*/
			return reply;
		}else if("sendCtrlMsg".equals(sysCallMsg.getMethod())){
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null);
			//try{
			//lock.lock();
				ControlMessage ctrlMsg = Json.loads(sysCallMsg.getJSONValue(), ControlMessage.class);
				sendCtrlMsg(ctrlMsg);
			/*}catch(Exception e){
				reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null, e.getMessage(), SystemCallErrorType.FATAL);
			}finally{
				lock.unlock();
			}*/
			return reply;
		}else if("removeFilterInstance".equals(sysCallMsg.getMethod())){
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null);
			//try{
			//lock.lock();
			SimpleEntry<String, Integer> pair = Json.loads(sysCallMsg.getJSONValue(), new TypeToken< SimpleEntry<String, Integer> >() {}.getType());
			
			removeFilterInstance(pair);
			/*}catch(Exception e){
                                reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null, e.getMessage(), SystemCallErrorType.FATAL);
                        }finally{
                                lock.unlock();
                        }*/
			return reply;
		}
		return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,"System call unkown",SystemCallErrorType.FATAL);
	}
}
