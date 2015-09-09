package pt.quintans.ezSQL.orm.domain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SimpleEntityCache {
	private static ThreadLocal<HashMap<Object, Object>>	cache	= new ThreadLocal<HashMap<Object, Object>>();

	private static HashMap<Object, Object> getCache() {
		HashMap<Object, Object> c = cache.get();
		if (c == null) {
			c = new HashMap<Object, Object>();
			cache.set(c);
		}

		return c;
	}

	@SuppressWarnings("unchecked")
	public static <T> T get(T e) {
		return (T) getCache().get(e);
	}

	public static <T> List<T> collapse(List<T> entities) {
		List<T> list = new ArrayList<T>();
		Set<T> set = new HashSet<T>();
		for (T entity : entities) {
			if (!set.contains(entity)) {
				set.add(entity);
				list.add(entity);
			}
		}
		return list;
	}

	/**
	 * if the entity is already cached returns the cached entity, if not, caches the new entity
	 * and returns null as an indication that the entity was not in cache
	 * 
	 * @param <T>
	 * @param entity
	 * @return
	 */
	public static <T> T cache(T entity) {
		if (entity != null) {

			T ent = get(entity);
			// nao encontrou
			if (ent == null) {
				// registers
				put(entity);
				return null;
			} else
				// found
				return ent;

		} else
			return null;
	}

	public static void put(Object entity) {
		if (entity != null) {
			getCache().put(entity, entity);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> T remove(T entity) {
		if (entity != null) {
			return (T) getCache().remove(entity);
		}

		return null;
	}

	public static boolean isClear() {
		return cache.get() == null;
	}

	public static void clear() {
		cache.set(null);
	}

}