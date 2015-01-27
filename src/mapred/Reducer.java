package mapred;

import java.util.Iterator;

import dpm.BasicProcess;
 
public abstract class Reducer<IKType, IVType, OKType, OVType> extends BasicProcess {
	public abstract void reduce(IKType key, Iterator<IVType> values, OutputCollector<OKType, OVType> output);
}
