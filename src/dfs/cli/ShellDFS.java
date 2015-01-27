package dfs.cli;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;

import java.util.Set;
import java.util.HashSet;

import java.util.concurrent.TimeUnit;

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

import dfs.DFSInterface;
import dfs.FileDescriptor;
import dfs.FileMode;

public class ShellDFS extends ShellApplication {
	private ModuleControllerInterface moduleController;
	private DFSInterface dfs;

	public void run(String []args){
		
		this.dfs = new DFSInterface(getSystemCallInterface());

		Options options = new Options();
		//options.addOption("l", "list", false, "list files");
		//options.addOption("add", true, "add a new module");
		options.addOption(OptionBuilder.withLongOpt( "list" )
                                .withDescription( "list the content of a given path" )
                                .hasArg()
                                .withArgName("path")
                                .create("l"));
		options.addOption(OptionBuilder.withLongOpt( "mkdir" )
                                .withDescription( "create a path of folders" )
                                .hasArg()
                                .withArgName("path")
                                .create());
		options.addOption(OptionBuilder.withLongOpt( "upload" )
                                .withDescription( "upload a file into the DFS" )
                                .hasArgs(2)
                                .withArgName("file")
                                .create("u"));
		options.addOption(OptionBuilder.withLongOpt( "download" )
                                .withDescription( "download a file from the DFS" )
                                .hasArgs(2)
                                .withArgName("file")
                                .create("d"));
		options.addOption(OptionBuilder.withLongOpt( "remove" )
                                .withDescription( "remove a file from the DFS" )
                                .hasArg()
                                .withArgName("file")
                                .create("rm"));

		options.addOption(OptionBuilder.withLongOpt( "size" )
                                .withDescription( "size of a file from the DFS" )
                                .hasArg()
                                .withArgName("file")
                                .create("s"));

		CommandLineParser parser = new BasicParser();
		try{
			CommandLine cmd = parser.parse(options, args);
			long startTime = System.nanoTime();
			if(cmd.hasOption("l")){
				String path = cmd.getOptionValue("l");
				Set<String> files = null;
				if(path!=null){
					files = this.dfs.list(path);
				}else {
					files = this.dfs.list();
				}
				if(files!=null){
					System.out.println(Json.dumps(files));
				}
			}else if(cmd.hasOption("mkdir")){
				String path = cmd.getOptionValue("mkdir");
				if(path!=null){
					this.dfs.mkdir(path);
				}
			}else if(cmd.hasOption("u")){
				String []paths = cmd.getOptionValues("u");
				if(paths!=null){
					BufferedReader in = new BufferedReader(new FileReader(paths[0]));
					FileDescriptor fd = this.dfs.open(paths[1], FileMode.WRITER);
					String line;
					while( (line=in.readLine())!=null ){
						this.dfs.write(fd, line);
					}
					this.dfs.flush(fd);
					this.dfs.close(fd);
				}
			}else if(cmd.hasOption("d")){
				String []paths = cmd.getOptionValues("d");
				if(paths!=null){
					PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(paths[1])));
					FileDescriptor fd = this.dfs.open(paths[0], FileMode.READER);
					String line;
					while( (line=this.dfs.readLine(fd))!=null ){
						out.println(line);
					}
					this.dfs.close(fd);
					out.flush();
					out.close();
				}
			}else if(cmd.hasOption("s")){
				String fileName = cmd.getOptionValue("s");
				if(fileName!=null){
					FileDescriptor fd = this.dfs.open(fileName, FileMode.READER);
					long size = this.dfs.size(fd);
					this.dfs.close(fd);
					System.out.println("FileName: "+fileName);
					System.out.println("Size: "+size);
				}
			}else if(cmd.hasOption("rm")){
				String fileName = cmd.getOptionValue("rm");
				if(fileName!=null){
					this.dfs.remove(fileName);
				}
			}else{
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp(args[0], options );
			}
			long endTime = System.nanoTime();
			System.out.println("duration: "+TimeUnit.MILLISECONDS.convert(endTime-startTime, TimeUnit.NANOSECONDS)+"ms : "+TimeUnit.SECONDS.convert(endTime-startTime, TimeUnit.NANOSECONDS)+"s");
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
