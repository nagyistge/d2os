package d2os.kernel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Handler implementation for the system call server.
 */
@Sharable
public class SystemCallServerHandler extends ChannelInboundHandlerAdapter{
	private Map<String, ModuleExecutor> modules;

	public SystemCallServerHandler(Set<ModuleExecutor> handlers){
		this.modules = new ConcurrentHashMap<String, ModuleExecutor>();
		for(ModuleExecutor handler: handlers){
			this.modules.put(handler.getModuleName(), handler);
		}
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg){
		SystemCallRequest req = (SystemCallRequest)msg;
		System.out.println("Server received: "+req.module()+"."+req.method());
		SystemCallReply reply = new SystemCallReply(req.module(),req.method(),null,"Unknown Module: "+req.module(),SystemCallErrorType.WARNING);
		if(this.modules.containsKey(req.module()))
			reply = this.modules.get(req.module()).handleSystemCall(req);
		if(req.waitReply()){
			ctx.writeAndFlush(reply);
		}else{
			ctx.close();
		}
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx){
		//ctx.flush();
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause){
		cause.printStackTrace();
		ctx.close();
	}
}
