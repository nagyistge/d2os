
package d2os.util;

import java.lang.reflect.Type;

import com.google.gson.Gson;

public class Json {
	private static Gson gson = new Gson();

	// Suppress default constructor for noninstantiability
	private Json() {
		throw new AssertionError();
	}

	public static <T> String dumps(T obj){
		return gson.toJson(obj);
	}

	public static <T> String dumps(T obj, Type type){
		return gson.toJson(obj,type);
	}

	public static <T> T loads(String json, Class<T> classOfType){
		return gson.fromJson(json, classOfType);
	}

	public static <T> T loads(String json, Type type){
		return gson.fromJson(json, type);
	}
}
