package d2os.kernel;

import java.util.Set;
import java.util.HashSet;

import java.net.UnknownHostException;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkInterruptedException;

import org.apache.commons.lang.exception.NestableException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.concurrent.DefaultEventExecutorGroup; //for non-blocking event handler
import io.netty.handler.codec.serialization.ClassResolvers; //serialization
import io.netty.handler.codec.serialization.ObjectDecoder;  //serialization
import io.netty.handler.codec.serialization.ObjectEncoder;  //serialization

import d2os.kernel.info.NodeInfo;

import d2os.net.NetUtil;
import d2os.util.Logger;

public final class NodeManager extends ModuleExecutor {

	static final boolean SSL = System.getProperty("ssl") != null;

	private ZkClient zk;


	NodeManager(ZkClient zk){
		this.zk = zk;

		addSystemCallHandler("launch", new SystemCallHandler(){
			public SystemCallReply handle(SystemCallContext ctx, SystemCallArguments args){
				return ctx.reply(null);
			}
		});
		addSystemCallHandler("kill", new SystemCallHandler(){
			public SystemCallReply handle(SystemCallContext ctx, SystemCallArguments args){
				return ctx.reply(null);
			}
		});
	}

	public static void runServer(Configuration config, final Set<ModuleExecutor> modules) throws Exception {
		// Configure SSL.
		final SslContext sslCtx;
		if(SSL){
			SelfSignedCertificate ssc = new SelfSignedCertificate();
			sslCtx = SslContext.newServerContext(ssc.certificate(), ssc.privateKey());
		}else{
			sslCtx = null;
		}

		//Configure the server.
		EventLoopGroup bossGroup = new NioEventLoopGroup(1);
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		try{
			ServerBootstrap b = new ServerBootstrap();
			b.group(bossGroup, workerGroup)
				.channel(NioServerSocketChannel.class)
				.option(ChannelOption.SO_BACKLOG, 100)
				.handler(new LoggingHandler(LogLevel.INFO))
				.childHandler(new ChannelInitializer<SocketChannel>() {
				@Override
				public void initChannel(SocketChannel ch) throws Exception {
					ChannelPipeline p = ch.pipeline();
					if(sslCtx != null){
						p.addLast(sslCtx.newHandler(ch.alloc()));
					}
					//p.addLast(new LoggingHandler(LogLevel.INFO));
					//p.addLast(new EchoServerHandler());
					p.addLast(
						new ObjectEncoder(),
						new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
						p.addLast(new DefaultEventExecutorGroup(2), new SystemCallServerHandler(modules));
				}
			});

			// Start the server.
			ChannelFuture f = b.bind(config.getInt("master.port")).sync();

			// Wait until the server socket is closed.
			f.channel().closeFuture().sync();
		}finally{
			// Shut down all event loops to terminate all threads.
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
		}
	}

	public static void main(String[] args) throws Exception {	
		Options options = new Options();
		options.addOption(OptionBuilder.withLongOpt( "properties" )
                                .withDescription( "properties file name" )
                                .hasArg()
                                .withArgName("fileName")
                                .create("p"));
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;
		try{
			cmd = parser.parse(options, args);
		}catch(ParseException e){
			e.printStackTrace();
		}
		String propFileName = "d2os.properties";
		if(cmd.hasOption("p")){
			propFileName = cmd.getOptionValue("p");
		}
		Configuration config = null;

		try{
			config = new PropertiesConfiguration(propFileName);
		}catch(NestableException e){
			e.printStackTrace();
			System.exit(1);
		}

		Environment env = Environment.loadFromXMLFile(config.getString("env.file"));
		Logger.info("Environment loaded");
		String localNodeName = NetUtil.getLocalHostName();
		Logger.info("Local Node Name: "+localNodeName);
		NodeInfo localNodeInfo = env.getMasterInfo(localNodeName);
		if(localNodeInfo==null){
			throw new UnknownHostException("Host '"+localNodeName+"' not specified by the environment");
		}

		Logger.info("Starting ZooKeeper client");
		Logger.info("Updating ZooKeeper");

		String []zkServers = config.getStringArray("zk.servers");
		ZkClient zk = new ZkClient(zkServers[0]);
		Logger.info("Creating System Call Interface");
		SystemCallInterface sysCall = new SystemCallInterface(zkServers);

		String []modules = config.getStringArray("modules");

		Logger.info("Starting ModuleController");
		final NodeManager nodeManager = new NodeManager(zk);
		nodeManager.setSystemCallInterface(sysCall);

		final Set<ModuleExecutor> moduleExecutors = new HashSet<ModuleExecutor>();
		moduleExecutors.add(nodeManager);
		System.out.println("SystemCallServer: "+localNodeName+":"+config.getString("master.port"));
		runServer(config, moduleExecutors);
	}
}
