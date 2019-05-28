package com.github.quintans.jdbc.transformers;

import java.util.Collection;
import java.util.LinkedList;

/**
 * User: quintans
 * Date: 13-jun-2006
 * Time: 22:13:52
 * 
 * @param <T>
 */
public abstract class SimpleAbstractResultTransformer<T> implements IResultTransformer<T> {

	public SimpleAbstractResultTransformer() {
	}

	@Override
	public Collection<T> beforeAll() {
		return new LinkedList<T>();
	}

	@Override
	public void collect(Collection<T> result, T object) {
		result.add(object);
	}

	@Override
	public void afterAll(Collection<T> result) {
	}

}
