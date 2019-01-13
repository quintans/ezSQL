package com.github.quintans.ezSQL.transformers;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map;

import com.github.quintans.ezSQL.common.api.Updatable;
import com.github.quintans.ezSQL.dml.ColumnHolder;
import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.toolkit.io.BinStore;
import com.github.quintans.ezSQL.toolkit.io.TextStore;
import com.github.quintans.ezSQL.toolkit.utils.Holder;
import com.github.quintans.ezSQL.toolkit.utils.Misc;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

public class BeanTransformer<T> implements IQueryRowTransformer<T> {
	private Query query;

	private Class<T> clazz;

	private int paginationColumnOffset;

	private Map<String, BeanProperty> properties;
    
	public BeanTransformer(Query query, Class<T> clazz) {
		this.query = query;
		this.clazz = clazz;

		this.paginationColumnOffset = query.getDb().getDriver().paginationColumnOffset(query);
	}

	@Override
	public Query getQuery() {
		return this.query;
	}

	public Class<T> getClazz() {
		return this.clazz;
	}

	public int getPaginationColumnOffset() {
		return this.paginationColumnOffset;
	}

	@Override
	public Collection<T> beforeAll(ResultSetWrapper resultSet) {
		return new LinkedList<T>();
	}

	/**
	 * Populates the mapping with bean properties, using "tableAlias.propertyName" as key
	 * 
	 * @param resultSet
	 *            The resultSet
	 * @param tableAlias
	 *            The table alias. If <code>null</code> the mapping key is only "propertyName"
	 * @param type
	 *            The bean class
	 * @return            
	 */
	protected Map<String, BeanProperty> populateMapping(ResultSetWrapper rsw, String tableAlias, Class<?> type) {
		String prefix = (tableAlias == null ? "" : tableAlias + ".");

		Map<String, BeanProperty> mappings = BeanProperty.populateMapping(prefix, type);

		/*
		 * Matches the columns with the bean properties
		 */

		int[] types = rsw.getColumnTypes();
		if (types != null) {
			int count = 0;
			for (Function function : this.query.getColumns()) {
				count++;

				BeanProperty bp = null;
				if (tableAlias == null) {
	                String fa = function.getAlias();
					if (fa == null && function instanceof ColumnHolder) {
						ColumnHolder ch = (ColumnHolder) function;
						fa = ch.getColumn().getAlias();
					}
					bp = mappings.get(fa);
				} else {
                    String fa = function.getPseudoTableAlias();
                    if (tableAlias.equals(fa)) {
                        bp = mappings.get(prefix + function.getAlias());
                        if (function instanceof ColumnHolder) {
                            ColumnHolder ch = (ColumnHolder) function;
                            if (discardIfKeyIsNull() && bp != null && ch.getColumn().isKey()) {
                                bp.setKeyColumn(ch.getColumn());
                            }
                        }
                    }
				}

				if (bp != null) {
					bp.setPosition(count);
				}
			}
		}

		return mappings;
	}

	protected boolean discardIfKeyIsNull() {
		return false;
	}

	@Override
	public void onTransformation(Collection<T> result, T object) {
		result.add(object);
	}

	@Override
	public void afterAll(Collection<T> result) {
	}

	@Override
	public T transform(ResultSetWrapper rsw) throws SQLException {
		if (this.properties == null) {
			this.properties = populateMapping(rsw, null, this.clazz);
		}

		return toBean(rsw, this.clazz, this.properties, null);
	}

	protected <E> E toBean(ResultSetWrapper rsw, Class<E> type, Map<String, BeanProperty> properties, Holder<Boolean> wasEmpty) throws SQLException {
		E obj = null;
		try {
			obj = type.newInstance();
			for (Map.Entry<String, BeanProperty> entry : properties.entrySet()) {
				BeanProperty bp = entry.getValue();
				if (bp.getPosition() > 0) {
					int position = bp.getPosition() + this.paginationColumnOffset;
					Class<?> klass = bp.getKlass();
					Object val = driver().fromDb(rsw, position, klass);

					try {
						if (val != null) {
						    bp.invokeWriteMethod(obj, val);
							if(wasEmpty != null)
							    wasEmpty.set(Boolean.FALSE);
						} else if (bp.isKey()) // if any key is null, the bean is invalid
							return null;
					} catch (Exception e) {
                        throw new PersistenceException("Unable to write to " + obj.getClass().getSimpleName() + "." + bp.getWriteMethod().getName(), e);
					}
				}
			}
		} catch (Exception e) {
            throw new PersistenceException("Unable to create bean " + type.getCanonicalName(), e);
		}
		
		if(obj instanceof Updatable) {
		    ((Updatable) obj).clear();
		}
		
		return obj;
	}

	protected Driver driver() {
		return this.query.getDb().getDriver();
	}

	/**
	 * gets the value for the passed column
	 * 
	 * @param rs resultset
	 * @param column
	 *            the column for witch we want the value
	 * @return the value, in this case a Long
	 * @throws SQLException
	 */
	protected Byte getTiny(ResultSetWrapper rsw, int column) throws SQLException {
		return driver().toTiny(rsw, column);
	}

	protected Short getShort(ResultSetWrapper rsw, int column) throws SQLException {
		return driver().toShort(rsw, column);
	}

	protected Integer getInteger(ResultSetWrapper rsw, int column) throws SQLException {
		return driver().toInteger(rsw, column);
	}

    protected Long getLong(ResultSetWrapper rsw, int column) throws SQLException {
        return driver().toLong(rsw, column);
    }

	protected String getString(ResultSetWrapper rsw, int column) throws SQLException {
		return driver().toString(rsw, column);
	}

	protected Boolean getBoolean(ResultSetWrapper rsw, int column) throws SQLException {
		return driver().toBoolean(rsw, column);
	}

	protected Date getTime(ResultSetWrapper rsw, int column) throws SQLException {
		return driver().toTime(rsw, column);
	}

    protected Date getDate(ResultSetWrapper rsw, int column) throws SQLException {
        return driver().toDate(rsw, column);
    }

    protected Date getDateTime(ResultSetWrapper rsw, int column) throws SQLException {
        return driver().toDateTime(rsw, column);
    }

    protected Date getTimestamp(ResultSetWrapper rsw, int column) throws SQLException {
        return driver().toTimestamp(rsw, column);
    }

	protected Double getDecimal(ResultSetWrapper rsw, int column) throws SQLException {
		return driver().toDecimal(rsw, column);
	}

	protected BigDecimal getBigDecimal(ResultSetWrapper rsw, int column) throws SQLException {
		return driver().toBigDecimal(rsw, column);
	}

	protected TextStore getText(ResultSetWrapper rsw, int column) throws SQLException {
	    TextStore val = new TextStore();
        Misc.copy(driver().toText(rsw, column), val);
        return val;
	}

	protected BinStore getBin(ResultSetWrapper rsw, int column) throws SQLException {
		BinStore val = new BinStore();
        Misc.copy(driver().toBin(rsw, column), val);
        return val;
	}
}
