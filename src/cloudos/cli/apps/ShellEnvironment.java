package cloudos.cli.apps;

import java.io.IOException;

import java.util.Set;
import java.util.HashSet;

import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;

import org.apache.zookeeper.KeeperException;

import cloudos.cli.ShellApplication;

import cloudos.kernel.SystemCallInterface;
import cloudos.kernel.ResourceManagerInterface;

import cloudos.kernel.Environment;

import cloudos.util.Json;

public class ShellEnvironment extends ShellApplication {
	private ResourceManagerInterface resourceManager;

	public void run(String []args){
		this.resourceManager = new ResourceManagerInterface(getSystemCallInterface());

		Options options = new Options();
		options.addOption("lA", "list-all", false, "list All nodes");
		options.addOption("lm", "list-masters", false, "list masters");
		options.addOption("ls", "list-slaves", false, "list slaves");
		options.addOption(OptionBuilder.withLongOpt( "remove-slave" )
                                .withDescription( "remove slave" )
                                .hasArg()
                                .withArgName("NodeName")
                                .create("rs"));

		options.addOption(OptionBuilder.withLongOpt( "remove-master" )
                                .withDescription( "remove master" )
                                .hasArg()
                                .withArgName("NodeName")
                                .create("rm"));

		CommandLineParser parser = new BasicParser();
		try{
			CommandLine cmd = parser.parse(options, args);
			if(cmd.hasOption("lA")){
				Environment env = this.resourceManager.getEnvironmentAlive();
				HashSet<String> nodes = new HashSet<String>(env.getMasters());
				nodes.addAll(env.getSlaves());
				System.out.println(Json.dumps(nodes));
			}else if(cmd.hasOption("lm")){
				Set<String> masters = this.resourceManager.getMasterNames();
				System.out.println(Json.dumps(masters));
			}else if(cmd.hasOption("ls")){
				Set<String> masters = this.resourceManager.getSlaveNames();
				System.out.println(Json.dumps(masters));
			}else if(cmd.hasOption("rs")){
				String nodeName = cmd.getOptionValue("rs");
				if(nodeName!=null){
					this.resourceManager.removeSlave(nodeName);
				}
			}else if(cmd.hasOption("rm")){
				String nodeName = cmd.getOptionValue("rm");
				if(nodeName!=null){
					this.resourceManager.removeMaster(nodeName);
				}
			}else{
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp(args[0], options );
			}
		}catch(ParseException e){
			e.printStackTrace();
		}catch(IOException e){
			e.printStackTrace();
		}catch(KeeperException e){
			e.printStackTrace();
		}catch(InterruptedException e){
			e.printStackTrace();
		}
	}
}
