package cloudos.kernel;

import java.io.IOException;

import java.net.InetAddress;
import java.net.UnknownHostException;

import cloudos.kernel.info.NodeInfo;
import cloudos.util.Logger;

class NodeMonitor extends Thread {
	private NodeInfo nodeInfo;
	private boolean master;
	private ResourceManager resourceManager;
	private long sleepTime;
	private int pingTime;

	private InetAddress inetAddr;

	private boolean alive;
	private boolean loaded;
	int factor = 1;
	
	NodeMonitor(NodeInfo nodeInfo, boolean master, ResourceManager resourceManager, boolean alive, long sleepTime, int pingTime){
		this.nodeInfo = nodeInfo;
		this.master = master;
		this.resourceManager = resourceManager;
		this.alive = alive;
		this.sleepTime = sleepTime;
		this.pingTime = pingTime;
		try{
			this.inetAddr = InetAddress.getByName(nodeInfo.getAddress());
		}catch(UnknownHostException e){
			e.printStackTrace();
		}
		this.loaded = false;
	}

	NodeMonitor(NodeInfo nodeInfo, boolean master, ResourceManager resourceManager, boolean alive){
		this(nodeInfo, master, resourceManager, alive, 3000, 5000);
	}

	public void run(){
		while(true){
			try{Thread.sleep(sleepTime*factor);}catch(InterruptedException e){}
			try{
				if(!inetAddr.isReachable(pingTime)){
					if(factor<120){
						factor++;
					}
					if(alive){
						alive = false;
						loaded = false;
						//WARN ResourceManager that this Node is no longer responding
						if(master) this.resourceManager.onMasterDead(nodeInfo.getName());
						else this.resourceManager.onSlaveDead(nodeInfo.getName());
					}
				}else if(!alive){
					if(factor<120){
						factor++;
					}
					try{
						loaded = false;
						NodeExecutor.setup(nodeInfo, Global.getLocalNodeInfo());
						alive = true;
					}catch(IOException e){}
				}else if(!loaded){
					if(factor<120){
						factor++;
					}
					loaded = true;
					NodeAddress nodeAddr = new NodeAddress(nodeInfo, Global.getConfiguration().getInt("slave.port"));
					SystemCallReply reply = this.resourceManager.getSystemCallInterface().request(nodeAddr, NodeManager.class.getSimpleName(), "ping", null);
					if(reply!=null){
						Logger.info("ping-pong: "+nodeInfo.getName());
						factor = 1;
						//WARN ResourceManager that this Node has became available
						if(master) this.resourceManager.onMasterAlive(nodeInfo.getName());
						else this.resourceManager.onSlaveAlive(nodeInfo.getName());
					}
				}
			}catch(UnknownHostException e){
				e.printStackTrace();
				if(factor<120){
					factor++;
				}
				if(alive){
					alive = false;
					loaded = false;
					//WARN ResourceManager that this Node is no longer responding
					if(master) this.resourceManager.onMasterDead(nodeInfo.getName());
					else this.resourceManager.onSlaveDead(nodeInfo.getName());
				}
			}catch(IOException e){
				e.printStackTrace();
				if(factor<120){
					factor++;
				}
				if(alive){
					alive = false;
					loaded = false;
					//WARN ResourceManager that this Node is no longer responding
					if(master) this.resourceManager.onMasterDead(nodeInfo.getName());
					else this.resourceManager.onSlaveDead(nodeInfo.getName());
				}
			}
		}
	}
}
