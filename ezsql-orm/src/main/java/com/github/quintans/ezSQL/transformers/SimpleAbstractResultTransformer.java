package com.github.quintans.ezSQL.transformers;

import java.util.Collection;
import java.util.LinkedList;

/**
 * User: quintans
 * Date: 13-jun-2006
 * Time: 22:13:52
 * 
 * @param <T>
 */
public abstract class SimpleAbstractResultTransformer<T> extends AbstractResultTransformer<T> {

	@Override
	public Collection<T> beforeAll() {
		return new LinkedList<>();
	}

	@Override
	public void collect(Collection<T> result, T object) {
		super.collect(result, object);
		result.add(object);
	}

	@Override
	public void afterAll(Collection<T> result) {
	}

}
