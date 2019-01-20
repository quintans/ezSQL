package com.github.quintans.ezSQL.orm.domain;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.dml.ColumnHolder;
import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.orm.session.TransactionStore;
import com.github.quintans.ezSQL.toolkit.io.BinStore;
import com.github.quintans.ezSQL.toolkit.io.TextStore;
import com.github.quintans.ezSQL.toolkit.utils.Misc;
import com.github.quintans.ezSQL.transformers.IQueryRowTransformer;
import com.github.quintans.ezSQL.transformers.Navigation;
import com.github.quintans.ezSQL.transformers.NavigationNode;
import com.github.quintans.jdbc.transformers.IRowTransformer;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

public abstract class ORMTransformer<T> implements IQueryRowTransformer<T> {
	private Query query;
	private boolean reuse = false;

	private ResultSetWrapper resultSet;
	private Collection<T> result;

	private Navigation navigation;

	/*
	 * the foreign key that originated this secondary mapper
	 */
	private Association foreignKey = null;

	private Map<String, IRowTransformer<?>> cachedTransformers = null;

	private Map<String, Integer> positionalMapping = null;

	private int paginationColumnOffset;

	public ORMTransformer(Association foreignKey, ORMTransformer<?> other) {
		this.query = other.getQuery();
		this.reuse = other.isReuse();
		this.navigation = other.getNavigation();
		this.cachedTransformers = other.getCachedTransformers();
		this.positionalMapping = other.getPositionalMapping();
		this.resultSet = other.getResultSet();

		this.foreignKey = foreignKey;
		this.paginationColumnOffset = other.getPaginationColumnOffset();
	}

	public ORMTransformer(Query query, boolean reuse) {
		this.query = query;
		this.reuse = reuse;

		this.navigation = new Navigation();
		this.paginationColumnOffset = driver().paginationColumnOffset(query);
		/*
		 * for every column in the select returns its position like: "[table alias].[column alias]", [position]
		 */
		this.positionalMapping = new HashMap<String, Integer>();
		int count = 0;
		for (Function function : query.getColumns()) {
			if (function instanceof ColumnHolder) {
				ColumnHolder ch = (ColumnHolder) function;
				this.positionalMapping.put(ch.getTableAlias() + "." + ch.getColumn().getAlias(), ++count);
			}
		}
	}

	protected Driver driver() {
		return this.query.getDb().getDriver();
	}

	@Override
	public Query getQuery() {
		return this.query;
	}

	@Override
	public void setQuery(Query query) {
		this.query = query;
	}

	public ResultSetWrapper getResultSet() {
		return this.resultSet;
	}

	public boolean isReuse() {
		return this.reuse;
	}

	public Collection<T> getResult() {
		return this.result;
	}

	public Navigation getNavigation() {
		return this.navigation;
	}

	public int getPaginationColumnOffset() {
		return this.paginationColumnOffset;
	}

	public Map<String, IRowTransformer<?>> getCachedTransformers() {
		return this.cachedTransformers;
	}

	@Override
	public Collection<T> beforeAll(ResultSetWrapper resultSet) {
		this.resultSet = resultSet;
		this.navigation.rewind();
		this.navigation.prepare(this.query, this.reuse);

		TransactionStore.hold();
		SimpleEntityCache.clear();

		this.result = this.reuse ? new LinkedHashSet<T>() : new ArrayList<T>();
		return this.result;
	}

	@Override
	public void onTransformation(Collection<T> result, T object) {
		this.navigation.rewind();

		if (object != null && (!isReuse() || !result.contains(object)))
			result.add(object);
	}

	@Override
	public void afterAll(Collection<T> result) {
		TransactionStore.resume();
		SimpleEntityCache.clear();
		this.navigation.dispose();
	}

	@Override
	public abstract T transform(ResultSetWrapper rsw) throws SQLException;

	/**
	 * return the list o current branches and moves forward to the next list
	 * 
	 * @return the current list of branches
	 */
	protected List<Association> forwardBranches() {
		List<NavigationNode> assocs = this.navigation.getBranches();
		List<Association> list = null;
		if (assocs != null) {
			list = new ArrayList<Association>();
			for (NavigationNode assoc : assocs)
				list.add(assoc.getForeignKey());
		}
		this.navigation.forward(); // move to next branches
		return list;
	}

	/**
	 * returns the cached RowTransformer associated with IBaseDAO<br>
	 * if the RowTransformer is not yet cached, it is created and cached
	 * 
	 * @param foreignKey
	 *            the association for witch I want to load the entity
	 * @param dao
	 *            the IBaseDAO
	 * @return the RowTransformer
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	protected <E> E loadEntity(Association foreignKey, IORMRowTransformerFactory<E> dao) throws SQLException {
		if (this.cachedTransformers == null)
			this.cachedTransformers = new HashMap<String, IRowTransformer<?>>();

		String path = foreignKey.genericPath();
		IRowTransformer<E> rt = (IRowTransformer<E>) this.cachedTransformers.get(path);
		if (rt == null) {
			rt = dao.createTransformer(foreignKey, this);
			this.cachedTransformers.put(path, rt);
		}

		return rt.transform(getResultSet());
	}

	/**
	 * gets the position in the resultset<br>
	 * the column is calculate using the current branche
	 * 
	 * @param column
	 *            the table column we want
	 * @return the position in the resultset
	 */
	protected int getPosition(Column<?> column) {
		String alias = null;
		if (this.foreignKey == null)
			alias = this.query.getTableAlias();
		else
			alias = this.foreignKey.getAliasTo();
		return this.positionalMapping.get(alias + "." + column.getAlias()) + this.paginationColumnOffset;
	}

	public Map<String, Integer> getPositionalMapping() {
		return this.positionalMapping;
	}

	/**
	 * gets the value for the passed column
	 * 
	 * @param column
	 *            the column for witch we want the value
	 * @return the value, in this case a Long
	 * @throws SQLException
	 */
    protected Byte getTiny(Column<?> column) throws SQLException {
        return driver().toTiny(this.resultSet, getPosition(column));
    }

    protected Short getShort(Column<?> column) throws SQLException {
        return driver().toShort(this.resultSet, getPosition(column));
    }

    protected Integer getInteger(Column<?> column) throws SQLException {
        return driver().toInteger(this.resultSet, getPosition(column));
    }

    protected Long getLong(Column<?> column) throws SQLException {
        return driver().toLong(this.resultSet, getPosition(column));
    }

    protected String getString(Column<?> column) throws SQLException {
        return driver().toString(this.resultSet, getPosition(column));
    }

    protected Boolean getBoolean(Column<?> column) throws SQLException {
        return driver().toBoolean(this.resultSet, getPosition(column));
    }

    protected Date getTime(Column<?> column) throws SQLException {
        return driver().toTime(this.resultSet, getPosition(column));
    }

    protected Date getDate(Column<?> column) throws SQLException {
        return driver().toDate(this.resultSet, getPosition(column));
    }

    protected Date getTimestamp(Column<?> column) throws SQLException {
        return driver().toTimestamp(this.resultSet, getPosition(column));
    }

    protected Double getDecimal(Column<?> column) throws SQLException {
        return driver().toDecimal(this.resultSet, getPosition(column));
    }

    protected BigDecimal getBigDecimal(Column<?> column) throws SQLException {
        return driver().toBigDecimal(this.resultSet, getPosition(column));
    }

    protected BinStore getText(Column<?> column) throws SQLException {
        TextStore val = new TextStore();
        Misc.copy(driver().toText(this.resultSet, getPosition(column)), val);
        return val;
    }

    protected BinStore getBin(Column<?> column) throws SQLException {
        BinStore val = new BinStore();
        Misc.copy(driver().toBin(this.resultSet, getPosition(column)), val);
        return val;
    }
}
