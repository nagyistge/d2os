package d2os.kernel;

//import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.util.concurrent.DefaultEventExecutorGroup; //for non-blocking event handler
import io.netty.handler.codec.serialization.ClassResolvers; //serialization
import io.netty.handler.codec.serialization.ObjectDecoder;  //serialization
import io.netty.handler.codec.serialization.ObjectEncoder;  //serialization
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler; //channel event handler

public class SystemCallFuture {
	private final NodeAddress nodeAddr;
	private final SystemCallRequest sysCallReq;
	private final SystemCallContext sysCallContext;
	private SystemCallReply reply = null;
	
	private ChannelFuture channelFuture;

	SystemCallFuture(final NodeAddress nodeAddr, final SystemCallRequest sysCallReq) throws InterruptedException {
		this.sysCallReq = sysCallReq;
		this.sysCallContext = new SystemCallContext(sysCallReq);
		this.nodeAddr = nodeAddr;

		final boolean SSL = false;
		// Configure SSL.git
		final SslContext sslCtx;
		if(SSL){
			sslCtx = null;// SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE);
		}else{
			sslCtx = null;
		}

        // Configure the client.
        EventLoopGroup group = new NioEventLoopGroup();
	final SystemCallFuture future = this;
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
             .channel(NioSocketChannel.class)
             .option(ChannelOption.TCP_NODELAY, true)
             .handler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception {
                     ChannelPipeline p = ch.pipeline();
                     if (sslCtx != null) {
                         p.addLast(sslCtx.newHandler(ch.alloc(), nodeAddr.getNodeInfo().getAddress(), nodeAddr.getPort()));
                     }
                     //p.addLast(new LoggingHandler(LogLevel.INFO));
                     //p.addLast(new EchoClientHandler());
                     p.addLast(
                            new ObjectEncoder(),
                            new ObjectDecoder(ClassResolvers.cacheDisabled(null)) );
					p.addLast(new DefaultEventExecutorGroup(2), new SimpleChannelInboundHandler<SystemCallReply>(){
						@Override
						public void channelActive(ChannelHandlerContext ctx) {	
							ctx.writeAndFlush(sysCallReq);
						}
						@Override
						public void channelRead0(ChannelHandlerContext ctx, SystemCallReply msg) {
							setReply(msg);
							ctx.close();
						}
						@Override
						public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
							// Close the connection when an exception is raised.
							//cause.printStackTrace();
							setReply(getContext().error(cause.getMessage(), SystemCallErrorType.FATAL));
							ctx.close();
						}
					});
                 }
             });

            // Start the client.
            ///this.channelFuture = b.connect(nodeAddr.getNodeInfo().getAddress(), nodeAddr.getPort()).sync();
			ChannelFuture f = b.connect(nodeAddr.getNodeInfo().getAddress(), nodeAddr.getPort()).sync();

            // Wait until the connection is closed.
            //f.channel().closeFuture().sync();
			this.channelFuture = f.channel().closeFuture();
        } finally {
            // Shut down the event loop to terminate all threads.
            group.shutdownGracefully();
        }
		
	}

	void setReply(SystemCallReply reply){
		this.reply = reply;
	}

	SystemCallContext getContext(){
		return this.sysCallContext;
	}

	public SystemCallReply getNow(){
		return this.reply;
	}

}
