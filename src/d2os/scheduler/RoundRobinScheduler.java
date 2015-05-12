package d2os.scheduler;

import java.util.Set;
import java.util.ArrayList;

import d2os.kernel.Scheduler;

public class RoundRobinScheduler extends Scheduler {
	private int nextNode;

	public RoundRobinScheduler(){
		this.nextNode = 0;
	}

	public String nextNodeName(Set<String> nodes){
		if(nodes.size()==0)return null;
		if(this.nextNode>=nodes.size()){
			this.nextNode = 0;
		}
		String nodeName = (new ArrayList<String>(nodes)).get(this.nextNode);
		this.nextNode++;
		return nodeName;
	}
}
