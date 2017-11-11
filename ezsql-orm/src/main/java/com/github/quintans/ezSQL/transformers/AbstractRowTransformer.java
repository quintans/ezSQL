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

public abstract class AbstractRowTransformer<T> implements IRowTransformer<T> {
	private AbstractDb db;
	private boolean fetchSqlTypes;
    
	public AbstractRowTransformer() {
	}

	public AbstractRowTransformer(AbstractDb db) {
		this(db, false);
	}

	public AbstractRowTransformer(AbstractDb db, boolean fetchSqlTypes) {
		this.db = db;
		this.fetchSqlTypes = fetchSqlTypes;
	}

	@Override
	public boolean isFetchSqlTypes() {
		return this.fetchSqlTypes;
	}

	@Override
	public Collection<T> beforeAll(final ResultSet resultSet) {
		return null;
	}

	@Override
	abstract public T transform(ResultSet rs, int[] columnTypes) throws SQLException;

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

	public Object toIdentity(ResultSet rs, int columnIndex) throws SQLException {
		return driver().toIdentity(rs, columnIndex);
	}

	public Boolean toBoolean(ResultSet rs, int columnIndex) throws SQLException {
		return driver().toBoolean(rs, columnIndex);
	}

	public String toString(ResultSet rs, int columnIndex) throws SQLException {
		return driver().toString(rs, columnIndex);
	}

	public Byte toTiny(ResultSet rs, int columnIndex) throws SQLException {
		return driver().toTiny(rs, columnIndex);
	}

	public Short toShort(ResultSet rs, int columnIndex) throws SQLException {
		return driver().toShort(rs, columnIndex);
	}

	public Integer toInteger(ResultSet rs, int columnIndex) throws SQLException {
		return driver().toInteger(rs, columnIndex);
	}

    public Long toLong(ResultSet rs, int columnIndex) throws SQLException {
        return driver().toLong(rs, columnIndex);
    }

	public Double toDecimal(ResultSet rs, int columnIndex) throws SQLException {
		return driver().toDecimal(rs, columnIndex);
	}

	public BigDecimal toBigDecimal(ResultSet rs, int columnIndex) throws SQLException {
		return driver().toBigDecimal(rs, columnIndex);
	}

	public java.util.Date toTime(ResultSet rs, int columnIndex) throws SQLException {
		return driver().toTime(rs, columnIndex);
	}

    public java.util.Date toDate(ResultSet rs, int columnIndex) throws SQLException {
        return driver().toDate(rs, columnIndex);
    }

    public java.util.Date toDateTime(ResultSet rs, int columnIndex) throws SQLException {
        return driver().toDateTime(rs, columnIndex);
    }

    public java.util.Date toTimestamp(ResultSet rs, int columnIndex) throws SQLException {
        return driver().toTimestamp(rs, columnIndex);
    }

	public TextStore toText(ResultSet rs, int columnIndex) throws SQLException {
        TextStore val = new TextStore();
        Misc.copy(driver().toText(rs, columnIndex), val);
        return val;
	}

	public BinStore toBin(ResultSet rs, int columnIndex) throws SQLException {
	    BinStore val = new BinStore();
        Misc.copy(driver().toBin(rs, columnIndex), val);
        return val;
	}

}
