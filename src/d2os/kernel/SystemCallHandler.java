package d2os.kernel;

public interface SystemCallHandler {
	public SystemCallReply handle(SystemCallContext ctx, SystemCallArguments args);
}
