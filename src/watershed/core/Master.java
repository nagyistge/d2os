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

import java.util.concurrent.TimeUnit;

import java.util.AbstractMap.SimpleEntry;

import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;

import cloudos.kernel.ModuleMaster;
import cloudos.kernel.Global;
import cloudos.kernel.SystemCallRequest;
import cloudos.kernel.SystemCallReply;
import cloudos.kernel.SystemCallErrorType;
//import cloudos.kernel.ExecutionController;
import cloudos.kernel.NodeAddress;
import cloudos.kernel.SystemCallInterface;

import cloudos.scheduler.RandomScheduler;
import cloudos.scheduler.RoundRobinScheduler;

import cloudos.util.Json;
import cloudos.util.Logger;

import watershed.core.info.ModuleInfo;
import watershed.core.info.ModuleLoadInfo;
import watershed.core.info.SenderLoadInfo;

public class Master extends ModuleMaster {
	private Map<String, ModuleInfo> modules;
	private Map<String, Long> modulesTime;
	private Map<String, List<String>> inputs;
	private Map<String, List<String>> outputs;
	private Map<String, Map<Integer, String>> instancesLocation;
	private Map<String, Map<Integer, Boolean>> instancesHalt;
	private RoundRobinScheduler scheduler;

	public void start(){
		super.start();
		this.modules = new HashMap<String, ModuleInfo>();
		this.modulesTime = new HashMap<String, Long>();
		this.instancesLocation = new HashMap<String, Map<Integer, String>>();
		this.instancesHalt = new HashMap<String, Map<Integer, Boolean>>();
		this.inputs = new HashMap<String, List<String>>();
		this.outputs = new HashMap<String, List<String>>();
		this.scheduler = new RoundRobinScheduler();
	}
	
	public void finish(){
		super.finish();
	}

	private void execute(ModuleInfo moduleInfo) throws IOException{
		Logger.info("executing: "+moduleInfo.getFilterInfo().getName());

		this.modules.put(moduleInfo.getFilterInfo().getName(), moduleInfo);

		Logger.info("setting input channels up");

		for(String inChannelName: moduleInfo.getInputChannelInfo().keySet()){
			if(!this.inputs.keySet().contains(inChannelName)){
				this.inputs.put(inChannelName, new ArrayList<String>());
			}
			this.inputs.get(inChannelName).add(moduleInfo.getFilterInfo().getName());
		}

		Logger.info("setting output channels up");
		for(String outChannelName: moduleInfo.getOutputChannelInfo().keySet()){
			if(!this.outputs.keySet().contains(outChannelName)){
				this.outputs.put(outChannelName, new ArrayList<String>());
			}
			this.outputs.get(outChannelName).add(moduleInfo.getFilterInfo().getName());
		}

		Map<Integer, String> instanceMap = new HashMap<Integer,String>();
		this.instancesLocation.put(moduleInfo.getFilterInfo().getName(), instanceMap);
		this.instancesHalt.put(moduleInfo.getFilterInfo().getName(), new HashMap<Integer,Boolean>());

		Logger.info("loading filter instances: "+moduleInfo.getInstances());

		for(int i = 0; i<moduleInfo.getInstances(); i++){
			ModuleLoadInfo loadInfo = new ModuleLoadInfo(moduleInfo, i);
			//TODO schedule a working slave node
			String nodeName = this.scheduler.nextNodeName(getEnvironment().getSlaves());

			//TODO improve this API interface 
			NodeAddress nodeAddr = new NodeAddress(getEnvironment().getSlaveInfo(nodeName), Global.getConfiguration().getInt("slave.port")); //get default slave port from global configuration
			SystemCallReply reply = getSystemCallInterface().request(nodeAddr, "watershed", "loadModule", loadInfo);
			if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
			
			instanceMap.put(new Integer(i), nodeName);
			this.instancesHalt.get(moduleInfo.getFilterInfo().getName()).put(new Integer(i), Boolean.FALSE);
		}

		Logger.info("loading input channels");

		for(String inChannelName: moduleInfo.getInputChannelInfo().keySet()){
			Logger.info("loading in-channel: "+inChannelName);
			if(this.outputs.keySet().contains(inChannelName)){
				for(String producerName: this.outputs.get(inChannelName)){
				Logger.info("loading in-channel: "+inChannelName+" from "+producerName);
					SenderLoadInfo loadInfo = new SenderLoadInfo(inChannelName, producerName, moduleInfo.getFilterInfo().getName(), moduleInfo.getInstances());
					loadInfo.setSenderInfo(moduleInfo.getInputChannelInfo(inChannelName).getSenderInfo());
					loadInfo.setEncoderInfoStack(moduleInfo.getInputChannelInfo(inChannelName).getEncoderInfoStack());
					Set<String> nodes = new HashSet<String>();
					for(Integer key : instancesLocation.get(producerName).keySet()){
						String nodeName = instancesLocation.get(producerName).get(key);
						nodes.add(nodeName);
					}
					Logger.info("loading channel in nodes: "+Json.dumps(nodes));
					Logger.info("loading info: "+Json.dumps(loadInfo));
					for(String nodeName:nodes){
						NodeAddress nodeAddr = new NodeAddress(getEnvironment().getSlaveInfo(nodeName), Global.getConfiguration().getInt("slave.port")); //get default slave port from global configuration
						SystemCallReply reply = getSystemCallInterface().request(nodeAddr, "watershed", "loadSender", loadInfo);
						if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
					}
				}
			}
		}

		Logger.info("loading output channels");

		for(String outChannelName: moduleInfo.getOutputChannelInfo().keySet()){
			if(this.inputs.keySet().contains(outChannelName)){
				for(String consumerName: this.inputs.get(outChannelName)){
					SenderLoadInfo loadInfo = new SenderLoadInfo(outChannelName, moduleInfo.getFilterInfo().getName(), consumerName, this.modules.get(consumerName).getInstances());
					loadInfo.setSenderInfo(this.modules.get(consumerName).getInputChannelInfo(outChannelName).getSenderInfo());
					loadInfo.setEncoderInfoStack(this.modules.get(consumerName).getInputChannelInfo(outChannelName).getEncoderInfoStack());
					Set<String> nodes = new HashSet<String>();
					for(Integer key : instancesLocation.get(moduleInfo.getFilterInfo().getName()).keySet()){
						String nodeName = instancesLocation.get(moduleInfo.getFilterInfo().getName()).get(key);
						nodes.add(nodeName);
					}
					//sends only one system call for each nodeName
					for(String nodeName:nodes){
						NodeAddress nodeAddr = new NodeAddress(getEnvironment().getSlaveInfo(nodeName), Global.getConfiguration().getInt("slave.port")); //get default slave port from global configuration
						SystemCallReply reply = getSystemCallInterface().request(nodeAddr, "watershed", "loadSender", loadInfo);
						if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
					}
				}
			}
		}
		this.modulesTime.put(moduleInfo.getFilterInfo().getName(), new Long(System.nanoTime()));
		Logger.info("FINISHED loading: "+moduleInfo.getFilterInfo().getName());
	}

	private void sendCtrlMsg(ControlMessage ctrlMsg) throws IOException{
		Logger.info("sending ctrl msg: "+Json.dumps(ctrlMsg));
		int instance = -1;
		String dst = ctrlMsg.getDestination();
		if(dst.indexOf(':')>=0){
			instance = Integer.parseInt(dst.substring(dst.indexOf(':')+1));
			dst = dst.substring(0,dst.indexOf(':'));
		}
		Deque<String> path = ctrlMsg.parsePath(dst);
		if(path==null) return;
		Map<Integer, String> locations = instancesLocation.get(path.peek());
		Set<String> nodes = new HashSet<String>();
		if(instance>=0){
			nodes.add(locations.get(new Integer(instance)));
		}else{
			for(Integer key: locations.keySet()){
				nodes.add(locations.get(key));
			}
		}
		Logger.info("sending ctrl msg to: "+Json.dumps(nodes));
		for(String nodeName: nodes){
			NodeAddress nodeAddr = new NodeAddress(getEnvironment().getSlaveInfo(nodeName), Global.getConfiguration().getInt("slave.port")); //get default slave port from global configuration
			SystemCallReply reply = getSystemCallInterface().request(nodeAddr, "watershed", "sendCtrlMsg", ctrlMsg);
			if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
		}
		Logger.info("DONE! sending ctrl msg");
	}

	public void haltFilterInstance(SimpleEntry<String, Integer> pair) throws IOException {
		Logger.info("halting instance: "+Json.dumps(pair));
		String filterName = pair.getKey();
		Integer instance = pair.getValue();
		ModuleInfo moduleInfo = this.modules.get(filterName);
		Map<Integer, Boolean> haltedInstances = this.instancesHalt.get(filterName);
		if(haltedInstances!=null){
			haltedInstances.put(instance, Boolean.TRUE);
			boolean allHalted = true;
			for(Integer key: haltedInstances.keySet()){
				if(!haltedInstances.get(key).booleanValue()){
					allHalted = false;
					break;
				}
			}
			if(allHalted){
				Logger.info("remove: "+filterName);
				remove(filterName); //remove filter
				//send a control message to the data consumer filters: "producersHalted"
				Set<String> outputChanns = moduleInfo.getOutputChannelInfo().keySet();
				for(String chann: outputChanns){
					//if nobody else produces in this channel and there are consumers, send a signal
					Logger.info("check if there is producer for output channel: "+chann);
					Logger.info("outputs: "+Json.dumps(this.outputs.keySet()));
					Logger.info("inputs: "+Json.dumps(this.inputs.keySet()));
					if(!this.outputs.keySet().contains(chann) && this.inputs.keySet().contains(chann)){
						for(String consumerName: this.inputs.get(chann)){
							ControlMessage ctrlMsg = new ControlMessage("/", "/"+consumerName+"/in/"+chann, "ProducersHalted");
							sendCtrlMsg(ctrlMsg);
						}
					}
				}
			}
		}
		Logger.info("DONE! halting instance");
	}

	public void remove(String filterName) throws IOException {
		Logger.info("removing: "+filterName);
		long endTime = System.nanoTime();
		long startTime = this.modulesTime.get(filterName).longValue();
		this.modulesTime.remove(filterName);
		Logger.info("duration: "+filterName+" : "+TimeUnit.MILLISECONDS.convert(endTime-startTime, TimeUnit.NANOSECONDS)+"ms : "+TimeUnit.SECONDS.convert(endTime-startTime, TimeUnit.NANOSECONDS)+"s");
		Map<Integer, String> locations = instancesLocation.get(filterName);
		if(locations!=null){
			for(Integer key: locations.keySet()){
				NodeAddress nodeAddr = new NodeAddress(getEnvironment().getSlaveInfo(locations.get(key)), Global.getConfiguration().getInt("slave.port")); //get default slave port from global configuration
				SimpleEntry<String, Integer> pair = new SimpleEntry<String, Integer>(filterName, key);
				SystemCallReply reply = getSystemCallInterface().request(nodeAddr, "watershed", "removeFilterInstance", pair);
				if(reply.getErrorMessage()!=null) throw new IOException(reply.getErrorMessage());
			}
		}
		ModuleInfo moduleInfo = this.modules.get(filterName);
		for(String chann: moduleInfo.getInputChannelInfo().keySet()){
			Logger.info("Removing input channel: "+chann);
			Logger.info("Registered filters: "+Json.dumps(this.inputs.get(chann)));
			Iterator<String> iter = this.inputs.get(chann).iterator();
			while(iter.hasNext()){
				if(iter.next().equals(filterName)){
					Logger.info("Removing filter register: "+filterName);
					iter.remove();
				}
			}
			Logger.info("Registered filters: "+Json.dumps(this.inputs.get(chann)));
			if(this.inputs.get(chann).isEmpty()){
				Logger.info("removing channel entry: "+chann);
				this.inputs.remove(chann);
			}
		}
		for(String chann: moduleInfo.getOutputChannelInfo().keySet()){
			Logger.info("Removing output channel: "+chann);
			Logger.info("Registered filters: "+Json.dumps(this.outputs.get(chann)));
			Iterator<String> iter = this.outputs.get(chann).iterator();
			while(iter.hasNext()){
				if(iter.next().equals(filterName)){
					Logger.info("Removing filter register: "+filterName);
					iter.remove();
				}
			}
			Logger.info("Registered filters: "+Json.dumps(this.outputs.get(chann)));
			if(this.outputs.get(chann).isEmpty()){
				Logger.info("removing channel entry: "+chann);
				this.outputs.remove(chann);
			}
		}
		this.modules.remove(filterName);
		this.instancesLocation.remove(filterName);
		this.instancesHalt.remove(filterName);
		Logger.info("DONE! removing filter");
	}
	
	public SystemCallReply handleSystemCall(SystemCallRequest sysCallMsg){
		if("execute".equals(sysCallMsg.getMethod())){
			ModuleInfo moduleInfo = Json.loads(sysCallMsg.getJSONValue(), ModuleInfo.class);
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null);
			try{
				execute(moduleInfo);
			}catch(IOException e){
				reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,e.getMessage(),SystemCallErrorType.FATAL);
			}
			return reply;
		}else if("sendCtrlMsg".equals(sysCallMsg.getMethod())){
			ControlMessage ctrlMsg = Json.loads(sysCallMsg.getJSONValue(), ControlMessage.class);
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null);
			try{
				sendCtrlMsg(ctrlMsg);
			}catch(IOException e){
				reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,e.getMessage(),SystemCallErrorType.FATAL);
			}
			return reply;
		}else if("remove".equals(sysCallMsg.getMethod())){
			String filterName = Json.loads(sysCallMsg.getJSONValue(), String.class);
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null);
			try{
				remove(filterName);
			}catch(IOException e){
				reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,e.getMessage(),SystemCallErrorType.FATAL);
			}
			return reply;
		}else if("getModuleNames".equals(sysCallMsg.getMethod())){
			String jsonValue = Json.dumps(this.modules.keySet());
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),jsonValue);
			//out.writeLine(Base64.encodeBase64String(StringUtils.getBytesUtf8(Json.dumps(reply))));
			//out.flush();
			return reply;
		}else if("haltFilterInstance".equals(sysCallMsg.getMethod())){
			SystemCallReply reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null);
			SimpleEntry<String, Integer> pair = Json.loads(sysCallMsg.getJSONValue(), new TypeToken< SimpleEntry<String, Integer> >() {}.getType());
			try{
				haltFilterInstance(pair);
			}catch(IOException e){
				reply = new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,e.getMessage(),SystemCallErrorType.FATAL);
			}
			return reply;
		}
		return new SystemCallReply(sysCallMsg.getModule(), sysCallMsg.getMethod(),null,"System call unkown",SystemCallErrorType.FATAL);
	}
}
