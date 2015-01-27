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
import cloudos.kernel.ModuleControllerInterface;

import cloudos.kernel.info.ModuleInfo;

import cloudos.util.Json;

public class ShellModule extends ShellApplication {
	private ModuleControllerInterface moduleController;

	public void run(String []args){
		this.moduleController = new ModuleControllerInterface(getSystemCallInterface());

		Options options = new Options();
		options.addOption("l", "list", false, "list modules");
		//options.addOption("add", true, "add a new module");
		options.addOption(OptionBuilder.withLongOpt( "add" )
                                .withDescription( "add a new module" )
                                .hasArg()
                                .withArgName("XMLFile")
                                .create());
		//options.addOption("rm", "remove", true, "remove a module");
		options.addOption(OptionBuilder.withLongOpt( "remove" )
                                .withDescription( "remove a module" )
                                .hasArg()
                                .withArgName("ModuleName")
                                .create("rm"));
		CommandLineParser parser = new BasicParser();
		try{
			CommandLine cmd = parser.parse(options, args);
			if(cmd.hasOption("l")){
				Set<String> modules = this.moduleController.getModuleNames();
				System.out.println(Json.dumps(modules));
			}else if(cmd.hasOption("add")){
				String xmlFileName = cmd.getOptionValue("add");
				if(xmlFileName!=null){
					ModuleInfo moduleInfo = ModuleInfo.loadFromXMLFile(xmlFileName);
					//System.out.println(Json.dumps(moduleInfo));
					this.moduleController.addModule(moduleInfo);
				}
			}else if(cmd.hasOption("rm")){
				String moduleName = cmd.getOptionValue("rm");
				if(moduleName!=null){
					this.moduleController.removeModule(moduleName);
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
