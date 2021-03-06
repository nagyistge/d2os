package d2os.scheduler;

import java.util.Set;
import java.util.ArrayList;

import org.apache.commons.lang.math.RandomUtils;

import d2os.kernel.Scheduler;

public class RandomScheduler extends Scheduler {

	public String nextNodeName(Set<String> nodes){
		if(nodes.size()==0)return null;
		return (new ArrayList<String>(nodes)).get(RandomUtils.nextInt(nodes.size()));
	}
}
