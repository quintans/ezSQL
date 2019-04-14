package com.github.quintans.ezSQL.toolkit.reflection;

import com.sun.beans.WeakCache;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class FieldUtils {
    // Static Caches to speed up introspection.
    private static final WeakCache<Class<?>, List<TypedField>> typedFieldCache = new WeakCache<>();

    public static void flushCaches() {
        synchronized (typedFieldCache) {
            typedFieldCache.clear();
        }
    }

    /**
     * there is an interesting project here https://github.com/jhalterman/typetools for generic type discovery
     */
    public static Class<?> getTypeGenericClass(Type type) throws ClassNotFoundException {
        if (type instanceof ParameterizedType) {
            ParameterizedType aType = (ParameterizedType) type;
            if (aType.getActualTypeArguments()[0] instanceof GenericArrayType) {
                Class<?> c = (Class<?>) ((GenericArrayType) aType.getActualTypeArguments()[0]).getGenericComponentType();
                return Class.forName("[L" + c.getName() + ";"); // hack
            } else {
                return (Class<?>) aType.getActualTypeArguments()[0];
            }
        }

        return null;
    }

    private static List<Field> getFields(Class<?> type) {
        List<Field> fields = new ArrayList<>();
        Class<?> i = type;
        while (i != null && i != Object.class) {
            for (Field field : i.getDeclaredFields()) {
                if (!field.isSynthetic()) {
                    field.setAccessible(true);
                    fields.add(field);
                }
            }
            i = i.getSuperclass();
        }

        return fields;
    }

    private static List<Method> getMethods(Class<?> type) {
        List<Method> fields = new ArrayList<>();
        Class<?> i = type;
        while (i != null && i != Object.class) {
            for (Method method : i.getDeclaredMethods()) {
                if (!method.isSynthetic()) {
                    method.setAccessible(true);
                    fields.add(method);
                }
            }
            i = i.getSuperclass();
        }

        return fields;
    }

    private static Method getter(List<Method> methods, Field field) {
        String name = field.getName();
        String methodName = "get" + name.substring(0, 1).toUpperCase() + (name.length() > 1 ? name.substring(1) : "");
        for (Method m : methods) {
            if (m.getName().equals(methodName) && m.getParameterTypes().length == 0 && m.getReturnType().isAssignableFrom(field.getType())) {
                return m;
            }
        }
        return null;
    }

    private static Method setter(List<Method> methods, Field field) {
        String name = field.getName();
        String methodName = "set" + name.substring(0, 1).toUpperCase() + (name.length() > 1 ? name.substring(1) : "");
        for (Method m : methods) {
            if (m.getName().equals(methodName) && m.getParameterTypes().length == 1 && m.getParameterTypes()[0].isAssignableFrom(field.getType())) {
                return m;
            }
        }
        return null;
    }

    public static TypedField getBeanTypedField(Class<?> start, String fieldName) {
        TypedField typedField = getTypedField(start, fieldName);
        if (typedField != null && typedField.isGettable()) {
            return typedField;
        } else {
            return null;
        }
    }

    public static TypedField getTypedField(Class<?> start, String fieldName) {
        List<TypedField> fields = getTypedFields(start);
        return fields.stream().filter(f -> f.getName().equals(fieldName)).findFirst().orElse(null);
    }

    public static List<TypedField> getBeanTypedFields(Class<?> start) {
        return getTypedFields(start).stream()
                .filter(TypedField::isGettable)
                .collect(Collectors.toList());
    }

    public static List<TypedField> getTypedFields(Class<?> start) {
        List<TypedField> typeFields;
        synchronized (typedFieldCache) {
            typeFields = typedFieldCache.get(start);
        }
        if (typeFields == null) {
            typeFields = buildTypedFields(start);
            synchronized (typedFieldCache) {
                typedFieldCache.put(start, typeFields);
            }
        }
        return typeFields;
    }

    private static List<TypedField> buildTypedFields(Class<?> start) {
        // get all the type variable mappings for the class tree
        List<Type[]> actualTypes = new LinkedList<>();
        List<Class<?>> rawTypes = new LinkedList<>();
        Class<?> klass = start;
        while (klass != null && klass != Object.class) {
            Type superclassType = klass.getGenericSuperclass();
            if (superclassType instanceof ParameterizedType) {
                ParameterizedType parameterizedType = (ParameterizedType) superclassType;
                actualTypes.add(0, parameterizedType.getActualTypeArguments());

                Class<?> rawType = (Class<?>) parameterizedType.getRawType();
                rawTypes.add(0, rawType);

                klass = (Class<?>) ((ParameterizedType) superclassType).getRawType();
            } else {
                klass = Object.class;
            }
        }

        List<Field> fields = getFields(start);
        List<Method> methods = getMethods(start);
        List<TypedField> typedFields = new ArrayList<>();

        for (Field field : fields) {
            Type genericFieldType = field.getGenericType();

            // creates a sublist starting at the field declaration class
            Class<?> begin = field.getDeclaringClass();
            int cnt = 0;
            List<Type[]> actualTypes2 = actualTypes;
            List<Class<?>> rawTypes2 = rawTypes;
            for (Class<?> rawType : rawTypes) {
                if (rawType == begin) {
                    break;
                }
                cnt++;
            }
            if (cnt > 0) {
                actualTypes2 = actualTypes.subList(cnt, actualTypes.size());
                rawTypes2 = rawTypes.subList(cnt, rawTypes.size());
            }
            Type norm = normalize(actualTypes2, rawTypes2, genericFieldType);
            typedFields.add(new TypedField(field, getter(methods, field), setter(methods, field), norm));
        }
        return typedFields;
    }

    private static Type normalize(List<Type[]> actualTypes, List<Class<?>> rawTypes, Type type) {
        if (type instanceof TypeVariable) {
            String fieldTypeName = type.getTypeName();
            Type t = searchActualType(actualTypes.iterator(), rawTypes.iterator(), fieldTypeName);
            return normalize(actualTypes, rawTypes, t);
        } else if (type instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) type;
            Type[] actualTypeArguments = pt.getActualTypeArguments();
            Type[] atas = new Type[actualTypeArguments.length];
            for (int i = 0; i < actualTypeArguments.length; i++) {
                Type t = actualTypeArguments[i];
                atas[i] = normalize(actualTypes, rawTypes, t);
            }

            return ParameterizedTypeImpl.make((Class<?>) pt.getRawType(), atas, pt.getOwnerType());
        } else {
            return type;
        }
    }

    private static Type searchActualType(Iterator<Type[]> actualTypesIter, Iterator<Class<?>> rawTypesIter, String fieldType) {
        if (rawTypesIter.hasNext()) {
            Class<?> klass = rawTypesIter.next();
            TypeVariable[] rawTypes = klass.getTypeParameters();
            for (int i = 0; i < rawTypes.length; i++) {
                TypeVariable rawType = rawTypes[i];
                if (rawType.getName().equals(fieldType)) {
                    Type[] actualTypes = actualTypesIter.next();
                    Type type = actualTypes[i];
                    if (type instanceof TypeVariable) {
                        TypeVariable typeVar = (TypeVariable) type;
                        return searchActualType(actualTypesIter, rawTypesIter, typeVar.getName());
                    } else {
                        return actualTypes[i];
                    }
                }
            }
        }
        //throw new RuntimeException("Unable to retrieve generic field of type " + fieldType);
        return null;
    }

}
