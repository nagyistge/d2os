### Install

#### ZooKeeper

#### Nebula


```xml
<?xml version="1.0"?>
<environment>
	<master>
		<node>
			<name>master1</name>
			<address>192.168.0.101</address>
			<workspace>/home/username1/master1/</workspace>
			<ssh>
				<user>username1</user>
				<password>p4sSw0rD</password>
				<port>22</port>
			</ssh>
		</node>
	</master>
   <slave>
		<node>
			<name>slave1</name>
			<address>192.168.0.103</address>
			<workspace>/home/user1/slave1/</workspace>
			<ssh>
				<user>user1</user>
				<password>p4sSw0rD</password>
				<port>22</port>
			</ssh>
		</node>
		<node>
			<name>slave2</name>
			<address>192.168.0.104</address>
			<workspace>/home/user2/slave2/</workspace>
			<ssh>
				<user>user2</user>
				<password>p4sSw0rD</password>
				<port>22</port>
			</ssh>
		</node>
   </slave>
</environment>
```

```
env.file = environment.xml
master.port = 1234
slave.port = 1235
zk.servers = 192.168.0.101
modules = dfs.xml, watershed.xml
```

It is important to note that the JAntill framework validates each host defined in the XML configuration file before running the application. Each invalid host, e.g. hosts that might be offline, will be ignored during the execution of JAnthill applications.

#### See Also

How to install Hadoop on Ubuntu: http://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-single-node-cluster/

OpenSSH Configuration on Ubuntu: https://help.ubuntu.com/community/SSH/OpenSSH/Configuring

### JAnthill Application

A simple JAnthill application consists of a XML configuration file of the application and the implementation of the filters and the streams. Although the following example makes use of just two filters, JAnthill does not limit to two filters, you can use as much filters in the pipeline as you need.

#### Word-Counter Example

The word-counter example is a very common example for the MapReduce framework introduction. The word-counter JAnthill application consists of two filters equivalent to the map and reduce functions. This application needs an input stream that reads from the Hadoop Distributed File System (HDFS), the labelled stream communication between the two filters and the HDFS output stream. These streams are available in the standard JAnthill API. The labelled stream communication can be performed via HDFS files or directly amongst the instances of the `MapFilter` and the `ReduceFilter` via network connections.

The `MapFilter` receives a line read from the HDFS file and produces a key-value output for the `ReduceFilter`. The output is the word with the count of a single occurrence. A stream can be read-only or write-only, this is why a `write` operation can throw a `StreamNotWritable` exception. The `MapFilter` needs only to implement the `process` method, leaving the `start` and `finish` methods empty.

```java
import java.io.IOException;

import java.util.AbstractMap.SimpleEntry;

import dcc.ufmg.anthill.Filter;
import dcc.ufmg.anthill.stream.StreamNotWritable;

public class MapFilter extends Filter<String, SimpleEntry<String,String> >{
	public void start(String hostName, int taskId){}
	public void process(String data){
		try{
			String []strs = data.split("\\W");//non-word characters split

			//for each word emits a pair <word, 1> counting one more occurrence of the word.
			//this example is the map phase of the count word MapReduce common application
			for(String word : strs){
				if(word.length()>0){
					SimpleEntry<String,String> pair = new SimpleEntry<String,String>(word, "1");
					getOutputStream().write(pair);
				}
			}
		}catch(StreamNotWritable e){
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	public void finish(){}
}
```

The `ReduceFilter` receives a key-value input and produces a key-value output. It keeps a counter for each word. After all entries have been processed, the `ReduceFilter` produces the output key-value counting the total occurrence of each word. It is interesting to observe that the output is performed in the `finish` method instead of the `process` method.

```java
import java.io.IOException;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;

import dcc.ufmg.anthill.Filter;
import dcc.ufmg.anthill.stream.StreamNotWritable;

public class ReduceFilter extends Filter< SimpleEntry<String,String> , SimpleEntry<String,String> >{
	private HashMap<String, Integer> wordCounter;

	public void start(String hostName, int taskId){
		wordCounter = new HashMap<String, Integer>();
	}
	public void process(SimpleEntry<String,String> data){
		String word = data.getKey();
		Integer count = new Integer(data.getValue());
		if(wordCounter.containsKey(word)){
			wordCounter.put(word, new Integer(wordCounter.get(word).intValue()+count.intValue()));
		}else{
			wordCounter.put(word, new Integer(count.intValue()));
		}
	}
	public void finish(){
		for(String word : wordCounter.keySet()){
			try{
				SimpleEntry<String,String> pair = new SimpleEntry<String,String>(word, wordCounter.get(word).toString());
				getOutputStream().write(pair);
			}catch(StreamNotWritable e){
				e.printStackTrace();
			}catch(IOException e){
				e.printStackTrace();
			}
		}
	}
}
```

The XML configuration file contains the path of the application Java file, and the definition of the filters and the streams, as well as their connections. It is important to note that the execution of the application is completely based on this configuration file.

```xml
<?xml version="1.0"?>
<app-settings name="NetWordCounter" path="/home/rcor/dev/Java/JAnthill/app/netWordCounter/" file="netWordCounter.jar">
	<file path="/home/rcor/dev/Java/JAnthill/app/netWordCounter/" name="app-settings.xml" />
	<file path="/home/rcor/dev/Java/JAnthill/" name="settings.xml" />

	<filter name="mapperfilter" class="MapFilter" />
	<filter name="reducerfilter" class="ReduceFilter" />

	<stream name="mapinput" class="dcc.ufmg.anthill.stream.hdfs.LineReader">
		<attr name="filename" value="pg5000.txt"/>
		<attr name="path" value="/home/hduser/gutenberg/"/>
	</stream>

	<stream name="netreader" class="dcc.ufmg.anthill.stream.net.StringKeyValueReader" />

	<stream name="netwriter" class="dcc.ufmg.anthill.stream.net.StringKeyValueWriter" />

	<stream name="reduceoutput" class="dcc.ufmg.anthill.stream.hdfs.StringKeyValueWriter">
		<attr name="path" value="/home/hduser/output/WordCounterOutput/"/>
		<attr name="divisor" value="1"/>
	</stream>

	<module name="mapper" filter="mapperfilter" input="mapinput" output="netwriter"  instances="1" />
	<module name="reducer" filter="reducerfilter" input="netreader" output="reduceoutput"  instances="1" />

	<flow name="labelledFlow" from="mapper" to="reducer" />

	<sequence>
		<item name="1" module="mapper" />
		<item name="2" module="reducer" />
	</sequence>

</app-settings>
```

The application configuration file is responsible for defining which filter and streams will be used and connected. It defines the filter `mapperfilter` with the implemented `MapFilter` class. The input stream, `mapinput`, will be the HDFS line reader stream from the standard JAnthill API, `dcc.ufmg.anthill.stream.hdfs.LineReader`, and the output stream, `netwriter`, will be the network based labelled stream, also from the standard JAnthill API, `dcc.ufmg.anthill.stream.net.StringKeyValueWriter`.
The relationship amongst the `MapFilter` and the proper input and output streams is defined in the `module` tag, as well as the number of transparent copies of this filter that will be instantiated. 
Usually, the streams from the standard API expects some pre-defined attributes.

The flow information is useful for the streams implementation to know which filter will receive the produced output. And finally, the sequence information is useful for the framework to know the order of executing the modules. Although this filters are executed simultaneously, since we are using direct communication amongst the filter instances, if we were using communication via HDFS files, we could use break sequence items. The following sequence tells that the `reducer` module will be executed only after the `mapper` module has already finished executing.

```xml
	<sequence>
		<item name="1" module="mapper" />
		<item name="2" break="true" />
		<item name="3" module="reducer" />
	</sequence>
```
