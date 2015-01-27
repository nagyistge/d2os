package cloudos.kernel;

import java.util.Set;

public abstract class Scheduler {
	public abstract String nextNodeName(Set<String> nodes);
}
