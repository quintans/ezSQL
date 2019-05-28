package com.github.quintans.ezSQL.transformers;

import com.github.quintans.ezSQL.common.api.Updatable;
import com.github.quintans.jdbc.transformers.IResultTransformer;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

import java.sql.SQLException;
import java.util.Collection;

public abstract class AbstractResultTransformer<T> implements IResultTransformer<T> {

	@Override
	public Collection<T> beforeAll() {
		return null;
	}

	@Override
	abstract public T transform(ResultSetWrapper rs) throws SQLException;

	@Override
	public void collect(Collection<T> result, T object) {
	    if(object instanceof Updatable) {
	        ((Updatable) object).clear();
	    }
	}

	@Override
	public void afterAll(Collection<T> result) {
	}

}
