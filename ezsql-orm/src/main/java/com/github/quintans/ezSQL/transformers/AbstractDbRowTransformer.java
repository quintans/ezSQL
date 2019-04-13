package com.github.quintans.ezSQL.transformers;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.common.api.Updatable;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.toolkit.io.BinStore;
import com.github.quintans.ezSQL.toolkit.io.TextStore;
import com.github.quintans.ezSQL.toolkit.utils.Misc;
import com.github.quintans.jdbc.transformers.IRowTransformer;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

public abstract class AbstractDbRowTransformer<T> implements IRowTransformer<T> {

	@Override
	public Collection<T> beforeAll(final ResultSetWrapper rsw) {
		return null;
	}

	@Override
	abstract public T transform(ResultSetWrapper rs) throws SQLException;

	@Override
	public void onTransformation(Collection<T> result, T object) {
	    if(object instanceof Updatable) {
	        ((Updatable) object).clear();
	    }
	}

	@Override
	public void afterAll(Collection<T> result) {
	}

}
