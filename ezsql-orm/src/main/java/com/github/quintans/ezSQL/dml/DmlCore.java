package com.github.quintans.ezSQL.dml;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.NullSql;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.exceptions.PersistenceException;
import com.github.quintans.ezSQL.transformers.BeanProperty;

public abstract class DmlCore<T> extends DmlBase {
    protected final static String OPTIMISTIC_LOCK_MSG = "No update was possible for this version of the data. Data may have changed.";
    protected final static String VERSION_SET_MSG = "Unable to set Version data.";
    protected final static String ID_UNDEFINED_MSG = "Field ID is undefined!";

	protected Class<?> lastBeanClass = null;
	protected Map<String, BeanProperty> lastMappings;

	protected Map<Column<?>, Function> values;
	protected Column<?>[] sets = null;
	
	private int batchLimit = 1000;

	public DmlCore(AbstractDb db, Table table) {
		super(db, table);
	}
    
	/**
	 * Sets the value by defining a parameter with the column alias
	 * This values can be raw values or more elaborated values like
     * UPPER(t0.Column) or AUTO(t0.ID)
     * 
	 * @param col
	 *            The column
	 * @param value
	 *            The value to set
	 * @return this
	 */
	@SuppressWarnings("unchecked")
	protected T _set(Column<?> col, Object value) {
	    if(value == null) {
	        value = col.getType();
	    } else if (col.getType() == NullSql.CLOB && value instanceof String) {
	        value = ((String) value).toCharArray();
	    }
	    
	    Function token = Function.converteOne(value);
	    replaceRaw(token);
	    token.setTableAlias(this.tableAlias);
	    
		// if the column was not yet defined, the sql changed
		if (defineParameter(col, token))
			this.rawSql = null;

		return (T) this;
	}
	
    public T setNull(Column<?> col) {
        return _set(col, null);
    }	

    public T setTrue(Column<Boolean> col) {
        return _set(col, Boolean.TRUE);
    }   

    public T setFalse(Column<Boolean> col) {
        return _set(col, Boolean.FALSE);
    }   

	@SuppressWarnings("unchecked")
	public T sets(Column<?>... columns) {
	    for(Column<?> col : columns){
	        if(!table.equals(col.getTable())) {
	            throw new PersistenceException("Column " + col + " does not belong to table " + table);
	        }
	        // start by setting columns as null
	        _set(col, null);
	    }
		this.sets = columns;
        this.rawSql = null;

		return (T) this;
	}

	@SuppressWarnings("unchecked")
	public T values(Object... values) {
		if (this.sets == null)
			throw new PersistenceException("Columns are not defined!");

		if (this.sets.length != values.length)
			throw new PersistenceException("The number of defined columns is diferent from the number of passed values!");

		if (this.sets != null) {
			int i = 0;
			for (Column<?> col : this.sets) {
				_set(col, values[i++]);
			}
		}

		return (T) this;
	}

	protected boolean defineParameter(Column<?> col, Function value) {
		if (!col.getTable().getName().equals(this.table.getName()))
			throw new PersistenceException(col + " does not belong to table " + this.table);

		if (this.values == null)
			this.values = new LinkedHashMap<Column<?>, Function>();

		Function tok = this.values.put(col, value);
		   // if it is a parameter remove it
	    if (tok != null) {
	        if (value.getOperator() == EFunction.PARAM && tok.getOperator() == EFunction.PARAM) {
	            /*
	                Replace one param by another
	            */
	            String oldKey = (String) tok.getValue();
	            String key = (String) value.getValue();
	            // change the new param name to the old param name
	            value.setValue(tok.getValue());
	            // update the old value to the new one
	            this.parameters.put(oldKey, this.parameters.get(key));
	            // remove the new token
	            this.parameters.remove(key);
	            // The replace of one param by another should not trigger a new SQL string
	            return false;
	        } else if (tok.getOperator() == EFunction.PARAM) {
	            // removes the previous token
	            this.parameters.remove((String) tok.getValue());
	        }
	    }
	    
	    return true;
	}

	public Map<Column<?>, Function> getValues() {
		return this.values;
	}

	protected int execute(Logger logger, String FQCN, Map<String, Object> parameters) {
	    // just in case;
	    simpleJdbc.closeBatch();
	    
		getSql();
		debugSQL(logger, FQCN, this.rawSql.getOriginalSql());

		Map<String, Object> pars = db.transformParameters(parameters);

		long now = System.nanoTime();
		int i = getSimpleJdbc().update(rawSql.getSql(), rawSql.buildValues(pars));
		debug(logger, FQCN, "result = %s", i);
		debugTime(logger, FQCN, now);
		this.sets = null;
		return i;
	}
	
    
    protected int[] batch(Logger logger, String FQCN, Map<String, Object> parameters){
        getSql();
        Map<String, Object> pars = db.transformParameters(parameters);      
        simpleJdbc.batch(this.rawSql.getSql(), rawSql.buildValues(pars));
        if(simpleJdbc.getPending() >= batchLimit) {
        	return flushBatch(logger, FQCN);
        }
        return null;
    }

    protected int[] flushBatch(Logger logger, String FQCN){
    	int[] result = null;
    	if(simpleJdbc.getPending() > 0) {
	        debugSQL(logger, FQCN, this.rawSql.getOriginalSql());
	        long now = System.nanoTime();
	        result = simpleJdbc.flushUpdate();
	        debugTime(logger, FQCN, now);
    	}
        return result;
    }
    
    protected void endBatch(Logger logger, String FQCN){
        flushBatch(logger, FQCN);
        simpleJdbc.closeBatch();
    }
    
    /**
     * statements that are still pending to execute
     * 
     * @return
     */
    public int getPending() {
        return simpleJdbc.getPending();
    }

	public int getBatchLimit() {
		return batchLimit;
	}

	/**
	 * Defines the amount of pending DML commands at witch the ezSQL will flush them to the Database. 
	 * @param batchLimit
	 */
	public void batchLimit(int batchLimit) {
		this.batchLimit = batchLimit;
	}
}
