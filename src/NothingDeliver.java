
import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import cloudos.kernel.Global;

import cloudos.kernel.SystemCallInterface;
import cloudos.kernel.ModuleControllerInterface;

import cloudos.util.Logger;

import watershed.core.ChannelDeliver;

public class NothingDeliver extends ChannelDeliver<String>{

	public void start(){
		super.start();
	}
	public void finish(){
		super.finish();
	}
}
