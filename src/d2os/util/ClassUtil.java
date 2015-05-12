package d2os.util;
/**
 * @author Rodrigo Caetano O. ROCHA
 * @date 25 July 2013
 */

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.MalformedURLException;

import java.util.List;
import java.util.ArrayList;

public class ClassUtil {
	
	//private static List<URL> paths = new ArrayList<URL>();
	private static DynamicURLClassLoader cl = new DynamicURLClassLoader();
	// Suppress default constructor for noninstantiability
	private ClassUtil() {
		throw new AssertionError();
	}

	public static Class<?> load(String className) throws MalformedURLException, ClassNotFoundException{
		//return Class.forName(className);
		return load(className,null);
	}
	
	public static void addClassPath(String jarFileName) throws MalformedURLException {
		File file  = new File(jarFileName);
		URL url = file.toURI().toURL();
		//paths.add(url);
		cl.addURL(url);
	}
	/*
	public static void removeClassPath(String jarFileName) throws MalformedURLException {
		File file  = new File(jarFileName);
		URL url = file.toURI().toURL();
		//paths.add(url);
		cl.removeURL(url);
	}
	*/
	public static Class<?> load(String className, String jarFileName) throws MalformedURLException, ClassNotFoundException{
		/*if(jarFileName==null){
			return load(className);
		}*/
		System.out.println("Loading class: "+className);
		URL url = null;
		if(jarFileName!=null){
			File file  = new File(jarFileName);
			url = file.toURI().toURL();
			cl.addURL(url);
			System.out.println("Loading class from: "+jarFileName);
		}
		/*URL[] urls = new URL[paths.size()+1];
		for(int i = 0; i<paths.size(); i++){
			urls[i] = paths.get(i);
		}
		urls[urls.length-1]=url;
		*/
		//ClassLoader cl = new URLClassLoader(urls);
		Class cls = cl.loadClass(className);
		//if(url!=null) cl.removeURL(url);
		return cls;
	}

}
