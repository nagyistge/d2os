package cloudos.scheduler;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;
import java.util.Collections;
import java.util.Comparator;

import org.apache.commons.math3.stat.Frequency;

import cloudos.util.Logger;

public class LessBusyScheduler {

	static <K,V extends Comparable<? super V>> List<Entry<K, V>> entriesSortedByValues(Map<K,V> map) {
		List<Entry<K,V>> sortedEntries = new ArrayList<Entry<K,V>>(map.entrySet());
		Collections.sort(sortedEntries, 
            new Comparator<Entry<K,V>>() {
                @Override
                public int compare(Entry<K,V> e1, Entry<K,V> e2) {
                    return e2.getValue().compareTo(e1.getValue());
                }
            }
		);
		return sortedEntries;
	}

	public static String scheduleNext(Set<String> nodes, List<String> busy){
		if(nodes==null || nodes.isEmpty()) return null;

		Frequency freq = new Frequency();
		for(String nodeName: busy){
			freq.addValue(nodeName);
		}
		Map<String, Long> freqCount = new HashMap<String, Long>();
		for(String nodeName:nodes){
			freqCount.put(nodeName, new Long(freq.getCount(nodeName)));
		}
		List<Entry<String,Long>> sortedFreq = entriesSortedByValues(freqCount);
		return sortedFreq.get(sortedFreq.size()-1).getKey();
	}
}
