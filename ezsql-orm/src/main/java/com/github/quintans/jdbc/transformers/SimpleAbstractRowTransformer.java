package com.github.quintans.jdbc.transformers;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.common.api.Updatable;

import java.util.Collection;
import java.util.LinkedList;

/**
 * User: quintans
 * Date: 13-jun-2006
 * Time: 22:13:52
 * 
 * @param <T>
 */
public abstract class SimpleAbstractRowTransformer<T> implements IRowTransformer<T> {

	public SimpleAbstractRowTransformer() {
	}

	@Override
	public Collection<T> beforeAll(final ResultSetWrapper resultSet) {
		return new LinkedList<T>();
	}

	@Override
	public void onTransformation(Collection<T> result, T object) {
	    if(object instanceof Updatable) {
	        ((Updatable) object).clear();
	    }
		result.add(object);
	}

	@Override
	public void afterAll(Collection<T> result) {
	}

}
