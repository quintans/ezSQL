package com.github.quintans.ezSQL.transformers;

import java.util.Collection;
import java.util.LinkedList;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.common.api.Updatable;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

/**
 * User: quintans
 * Date: 13-jun-2006
 * Time: 22:13:52
 * 
 * @param <T>
 */
public abstract class SimpleAbstractDbRowTransformer<T> extends AbstractDbRowTransformer<T> {

	@Override
	public Collection<T> beforeAll(final ResultSetWrapper resultSet) {
		return new LinkedList<T>();
	}

	@Override
	public void onTransformation(Collection<T> result, T object) {
		super.onTransformation(result, object);
		result.add(object);
	}

	@Override
	public void afterAll(Collection<T> result) {
	}

}
