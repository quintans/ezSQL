package pt.quintans.ezSQL.toolkit.utils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collection;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import pt.quintans.ezSQL.exceptions.PersistenceException;
import pt.quintans.ezSQL.toolkit.io.BinStore;

public class Misc {
    private static Logger LOGGER = Logger.getLogger(Misc.class);

	public static String toCamelCase(String name) {
		if (name != null) {
			StringBuilder result = new StringBuilder(name.substring(0, 1).toLowerCase());
			int x = name.length();
			boolean toUpper = false;
			for (int i = 2; i <= x; i++) {
				String letter = name.substring(i - 1, i);
				if (letter.equals("_")) {
					toUpper = true;
				} else {
					if (toUpper) {
						letter = letter.toUpperCase();
						toUpper = false;
					} else {
						letter = letter.toLowerCase();
					}
					result.append(letter);
				}
			}
			return result.toString();
		}

		return null;
	}

	public static boolean match(Object o1, Object o2) {
		if (o1 == o2) // even if both are null
			return true;
		else if (o1 != null)
			return o1.equals(o2);
		else
			return o2.equals(o1);
	}
	
	public static int length(Collection<?> coll) {
	    return coll == null ? 0 : coll.size();
	}
	
    public static int length(Object[] data) {
        return data == null ? 0 : data.length;
    }
    
    public static void callAnnotatedMethod(Class<? extends Annotation> annotation , Object o, Object... arguments){
        if(o == null)
            return;
        
        Method[] methods = o.getClass().getMethods();
        for(Method method : methods) {
            if(method.isAnnotationPresent(annotation)){
                try {
                    method.invoke(o, arguments);
                } catch (Exception e) {
                    LOGGER.error("Unable to invoke method annotated with " + annotation.getSimpleName(), e);
                }
            }
        }
    }
    
    public static void copy(InputStream in, BinStore bc) {
        if(in == null)
            return;
        
        try {
            bc.set(in);
        } catch (IOException e) {
            throw new PersistenceException("Unable to set stream into bytecache!", e);
        } finally {
            IOUtils.closeQuietly(in);
        }
    }
    
    public static String capitalizeFirst(String s){
        return s.substring(0, 1).toUpperCase() + s.substring(1);
    }
}
