package cloudos.kernel;

import java.io.IOException;
import java.io.File;

import java.net.MalformedURLException;

import org.apache.commons.io.FileUtils;

import cloudos.kernel.info.ModuleExecutorInfo;
import cloudos.kernel.info.ExecutorInfo;
import cloudos.util.ClassUtil;

class ExecutionController <ExecutorType extends DefaultExecutor> {
	private ExecutorInfo executorInfo;
	private ExecutorType executor;

	ExecutionController(ExecutorInfo executorInfo){
		this.executor = null;
		this.executorInfo = executorInfo;
		String path = null;
		if(executorInfo.getFileBase64()!=null){
			path = executorInfo.getName();
			try{
				executorInfo.writeFile(path);
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		try{
			/*Object obj = ClassUtil.load(executorInfo.getClassName(), executorInfo.getName()).newInstance();
			if( obj instanceof ExecutorType){
				executor = (ExecutorType)obj;
			}else{
				//TODO generate exception
			}*/
			this.executor = (ExecutorType)ClassUtil.load(executorInfo.getClassName(), path).newInstance();
			if(path!=null){
				ClassUtil.addClassPath(path);
			}
		}catch(MalformedURLException e){
			e.printStackTrace();
		}catch(ClassNotFoundException e){
			e.printStackTrace();
		}catch(InstantiationException e){
			e.printStackTrace();
		}catch(IllegalAccessException e){
			e.printStackTrace();
		}
	}

	ExecutorType getExecutor(){
		return this.executor;
	}

	void start(){
		this.executor.start();
	}

	void finish(){
		this.executor.finish();
		try{
			FileUtils.forceDelete(new File(this.executorInfo.getName()));
		}catch(IOException e){
			e.printStackTrace();
		}
	}
}
