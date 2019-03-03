package com.github.quintans.ezSQL.toolkit.utils;

import com.github.quintans.ezSQL.toolkit.io.BinStore;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;

public class Misc {
    private static Logger LOGGER = Logger.getLogger(Misc.class);

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

    public static void callAnnotatedMethod(Class<? extends Annotation> annotation, Object o, Object... arguments) {
        if (o == null)
            return;

        Method[] methods = o.getClass().getMethods();
        for (Method method : methods) {
            if (method.isAnnotationPresent(annotation)) {
                try {
                    method.invoke(o, arguments);
                } catch (Exception e) {
                    LOGGER.error("Unable to invoke method annotated with " + annotation.getSimpleName(), e);
                }
            }
        }
    }

    public static void copy(InputStream in, BinStore bc) {
        if (in == null)
            return;

        try {
            bc.set(in);
        } catch (IOException e) {
            throw new PersistenceException("Unable to set stream into bytecache!", e);
        } finally {
            IOUtils.closeQuietly(in);
        }
    }


    /**
     * there is an interesting project here https://github.com/jhalterman/typetools for generic type discovery
     *
     * @param genericType
     * @return
     * @throws ClassNotFoundException
     */
    public static Class<?> genericClass(Type genericType) throws ClassNotFoundException {
        if (genericType instanceof ParameterizedType) {
            ParameterizedType aType = (ParameterizedType) genericType;
            if (aType.getActualTypeArguments()[0] instanceof GenericArrayType) {
                Class<?> c = (Class<?>) ((GenericArrayType) aType.getActualTypeArguments()[0]).getGenericComponentType();
                return Class.forName("[L" + c.getName() + ";"); // hack
            } else
                return (Class<?>) aType.getActualTypeArguments()[0];
        }

        return null;
    }

    public static PropertyDescriptor getPropertyDescriptor(Class<?> klass, String name) {
        BeanInfo info;
        try {
            info = Introspector.getBeanInfo(klass);
        } catch (IntrospectionException e) {
            throw new PersistenceException("Unable to get bean information for " + klass.getCanonicalName(), e);
        }

        PropertyDescriptor[] props = info.getPropertyDescriptors();
        for (PropertyDescriptor p : props) {
            if (p.getName().equals(name)) {
                Method m = p.getReadMethod();
                if( m == null) {
                    throw new RuntimeException(klass.getSimpleName() + "#" + name + " has no read method");
                }
                makeAccessible(m);

                m = p.getWriteMethod();
                if( m == null) {
                    throw new RuntimeException(klass.getSimpleName() + "#" + name + " has no write method");
                }
                makeAccessible(m);
                return p;
            }
        }

        return null;
    }

    private static void makeAccessible(Method m) {
        if (!m.isAccessible()) {
            m.setAccessible(true);
        }
    }

}
