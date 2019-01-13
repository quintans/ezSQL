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
	private AbstractDb db;
	
	public AbstractDbRowTransformer(AbstractDb db) {
		this.db = db;
	}

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

	protected Driver driver() {
		return this.db.getDriver();
	}

	public Object toIdentity(ResultSetWrapper rs, int columnIndex) throws SQLException {
		return driver().toIdentity(rs, columnIndex);
	}

	public Boolean toBoolean(ResultSetWrapper rs, int columnIndex) throws SQLException {
		return driver().toBoolean(rs, columnIndex);
	}

	public String toString(ResultSetWrapper rs, int columnIndex) throws SQLException {
		return driver().toString(rs, columnIndex);
	}

	public Byte toTiny(ResultSetWrapper rs, int columnIndex) throws SQLException {
		return driver().toTiny(rs, columnIndex);
	}

	public Short toShort(ResultSetWrapper rs, int columnIndex) throws SQLException {
		return driver().toShort(rs, columnIndex);
	}

	public Integer toInteger(ResultSetWrapper rs, int columnIndex) throws SQLException {
		return driver().toInteger(rs, columnIndex);
	}

    public Long toLong(ResultSetWrapper rs, int columnIndex) throws SQLException {
        return driver().toLong(rs, columnIndex);
    }

	public Double toDecimal(ResultSetWrapper rs, int columnIndex) throws SQLException {
		return driver().toDecimal(rs, columnIndex);
	}

	public BigDecimal toBigDecimal(ResultSetWrapper rs, int columnIndex) throws SQLException {
		return driver().toBigDecimal(rs, columnIndex);
	}

	public java.util.Date toTime(ResultSetWrapper rs, int columnIndex) throws SQLException {
		return driver().toTime(rs, columnIndex);
	}

    public java.util.Date toDate(ResultSetWrapper rs, int columnIndex) throws SQLException {
        return driver().toDate(rs, columnIndex);
    }

    public java.util.Date toDateTime(ResultSetWrapper rs, int columnIndex) throws SQLException {
        return driver().toDateTime(rs, columnIndex);
    }

    public java.util.Date toTimestamp(ResultSetWrapper rs, int columnIndex) throws SQLException {
        return driver().toTimestamp(rs, columnIndex);
    }

	public TextStore toText(ResultSetWrapper rs, int columnIndex) throws SQLException {
        TextStore val = new TextStore();
        Misc.copy(driver().toText(rs, columnIndex), val);
        return val;
	}

	public BinStore toBin(ResultSetWrapper rs, int columnIndex) throws SQLException {
	    BinStore val = new BinStore();
        Misc.copy(driver().toBin(rs, columnIndex), val);
        return val;
	}

}
