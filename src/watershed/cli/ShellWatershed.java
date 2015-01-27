package watershed.cli;

import java.io.IOException;

import java.util.Set;
import java.util.HashSet;

import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;

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

import watershed.core.api.WatershedInterface;
import watershed.core.info.ModuleInfo;

import cloudos.util.Json;

public class ShellWatershed extends ShellApplication {
	//private ModuleControllerInterface moduleController;
	private WatershedInterface watershed;

	public void run(String []args){
		/*
		this.moduleController = new ModuleControllerInterface(getSystemCallInterface());
		SystemCallInterface sysCall = null;
		try{
			sysCall = this.moduleController.getModuleSystemCall("watershed");
		}catch(IOException e){
			e.printStackTrace();
			return;
		}
		if(sysCall==null) return;
		*/
		this.watershed = new WatershedInterface(getSystemCallInterface());

		Options options = new Options();
		options.addOption("l", "list", false, "list modules");
		//options.addOption("add", true, "add a new module");
		options.addOption(OptionBuilder.withLongOpt( "add" )
                                .withDescription( "add a new module" )
                                .hasArgs()
                                .withArgName("XMLFiles")
                                .create());
		//options.addOption("rm", "remove", true, "remove a module");
		options.addOption(OptionBuilder.withLongOpt( "remove" )
                                .withDescription( "remove a module" )
                                .hasArgs()
                                .withArgName("ModuleNames")
                                .create("rm"));
		CommandLineParser parser = new BasicParser();
		try{
			CommandLine cmd = parser.parse(options, args);
			if(cmd.hasOption("l")){
				Set<String> modules = this.watershed.getModuleNames();
				System.out.println(Json.dumps(modules));
			}else if(cmd.hasOption("add")){
				String []xmlFileNames = cmd.getOptionValues("add");
				if(xmlFileNames!=null){
					for(String xmlFileName: xmlFileNames){
						ModuleInfo moduleInfo = ModuleInfo.loadFromXMLFile(xmlFileName);
						//System.out.println(Json.dumps(moduleInfo));
						this.watershed.load(moduleInfo);
					}
				}
			}else if(cmd.hasOption("rm")){
				String []moduleNames = cmd.getOptionValues("rm");
				if(moduleNames!=null){
					for(String moduleName: moduleNames){
						this.watershed.remove(moduleName);
					}
				}
			}else{
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp(args[0], options );
			}
		}catch(ParseException e){
			e.printStackTrace();
		}catch(ParserConfigurationException e){
			e.printStackTrace();
		}catch(SAXException e){
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
