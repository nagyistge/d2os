package dpm;

import org.apache.commons.configuration.Configuration;

import cloudos.kernel.DefaultExecutor;

public abstract class MasterProcess extends DefaultExecutor {
	private ContainerController containerController;
	private Configuration conf;

	void setContainerController(ContainerController containerController){
		this.containerController = containerController;
	}

	public ContainerController getContainerController(){
		return this.containerController;
	}

	void setConfiguration(Configuration conf){
		this.conf = conf;
	}

	public Configuration getConfiguration(){
		return this.conf;
	}
}

