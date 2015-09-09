package pt.quintans.ezSQL.transformers;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import pt.quintans.ezSQL.db.Column;
import pt.quintans.ezSQL.exceptions.PersistenceException;

public class BeanProperty {
    private int position = 0;
    private String name;
    private Method writeMethod;
    private Method readMethod;
    private Class<?> klass;
    private int sqlType = 0;
    private Class<?> genericClass = null;
    private Column<?> keyColumn = null;
    
    public int getPosition() {
        return this.position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public int getSqlType() {
        return this.sqlType;
    }

    public void setSqlType(int sqlType) {
        this.sqlType = sqlType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setWriteMethod(Method method) {
        this.writeMethod = method;
    }

    public void setKlass(Class<?> klass) {
        this.klass = klass;
    }

    public Method getWriteMethod() {
        return this.writeMethod;
    }

    public Method getReadMethod() {
        return this.readMethod;
    }

    public void setReadMethod(Method readMethod) {
        this.readMethod = readMethod;
    }

    public Class<?> getKlass() {
        return this.klass;
    }

    public Class<?> getGenericClass() {
        return this.genericClass;
    }

    public void setGenericClass(Class<?> genericClass) {
        this.genericClass = genericClass;
    }

    public boolean isMany() {
        return this.genericClass != null;
    }

    public Column<?> getKeyColumn() {
        return keyColumn;
    }

    public void setKeyColumn(Column<?> keyColumn) {
        this.keyColumn = keyColumn;
    }
    
    public boolean isKey() {
        return keyColumn != null;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("BeanProperty [position=");
        builder.append(this.position);
        builder.append(", name=");
        builder.append(this.name);
        builder.append(", klass=");
        builder.append(this.klass);
        builder.append(", sqlType=");
        builder.append(this.sqlType);
        builder.append(", genericClass=");
        builder.append(this.genericClass);
        builder.append(", keyColumn=");
        builder.append(this.keyColumn);
        builder.append("]");
        return builder.toString();
    }
    
    public void invokeWriteMethod(Object obj, Object val) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        writeMethod.invoke(obj, val);
    }

    public Object invokeReadMethod(Object obj) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        return readMethod.invoke(obj);
    }

    public static Map<String, BeanProperty> populateMapping(String prefix, Class<?> type) {
        Map<String, BeanProperty> mappings = new LinkedHashMap<String, BeanProperty>();

        /*
         * finds out what methods are available
         */
        try {
            BeanInfo info = Introspector.getBeanInfo(type);
            PropertyDescriptor[] props = info.getPropertyDescriptors();
            String name;
            for (int i = 0; i < props.length; ++i) {
                PropertyDescriptor prop = props[i];
                name = prop.getName();
                if (!"class".equals(name)) {
                    Method setter = prop.getWriteMethod();
                    Method getter = prop.getReadMethod();
                    if (setter != null) {
                        BeanProperty bp = new BeanProperty();
                        if (!setter.isAccessible())
                            setter.setAccessible(true);
                        if (!getter.isAccessible())
                            getter.setAccessible(true);
                        bp.setKlass(prop.getPropertyType());
                        String mappingsKey = prefix == null || prefix.isEmpty() ? name : prefix + name;
                        // register bean property with prefix
                        mappings.put(mappingsKey, bp);
                        bp.setName(name);
                        bp.setWriteMethod(setter);
                        bp.setReadMethod(getter);
                        if (Collection.class.isAssignableFrom(bp.getKlass())) {
                            // gets the generic class
                            bp.setGenericClass(genericClass(bp.getWriteMethod()));
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new PersistenceException("Unable to get bean information for " + type.getCanonicalName(), e);
        }

        return mappings;
    }

    private static Class<?> genericClass(Method method) throws ClassNotFoundException {
        return genericClass(method.getGenericParameterTypes()[0]);
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
 
}
