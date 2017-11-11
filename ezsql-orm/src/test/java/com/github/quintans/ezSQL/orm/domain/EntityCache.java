package com.github.quintans.ezSQL.orm.domain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EntityCache {
	private static ThreadLocal<HashMap<EntityKeyHolder, CachedEntity>>	cache	= new ThreadLocal<HashMap<EntityKeyHolder, CachedEntity>>();

	private static HashMap<EntityKeyHolder, CachedEntity> getCache() {
		HashMap<EntityKeyHolder, CachedEntity> c = cache.get();
		if (c == null) {
			c = new HashMap<EntityKeyHolder, CachedEntity>();
			cache.set(c);
		}

		return c;
	}

	@SuppressWarnings("unchecked")
	public static <T extends CachedEntity> T get(EntityKeyHolder ekh) {
		return (T) getCache().get(ekh);
	}

	public static <T extends CachedEntity> List<T> collapse(List<T> entities) {
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
	@SuppressWarnings("unchecked")
	public static <T extends CachedEntity> T cache(T entity) {
		if (entity != null) {

			T ent = (T) get(entity.keyHolder());
			// nao encontrou
			if (ent == null) {
				// regista
				put(entity);
				return null;
			} else
				// encontrou
				return ent;

		} else
			return null;
	}

	public static <T extends CachedEntity> void put(T entity) {
		if (entity != null) {
			getCache().put(entity.keyHolder(), entity);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T extends CachedEntity> T remove(T entity) {
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