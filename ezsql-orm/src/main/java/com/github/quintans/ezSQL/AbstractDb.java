package com.github.quintans.ezSQL;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.github.quintans.ezSQL.common.api.Updatable;
import com.github.quintans.ezSQL.common.type.MyDate;
import com.github.quintans.ezSQL.common.type.MyDateTime;
import com.github.quintans.ezSQL.common.type.MyTime;
import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.NullSql;
import com.github.quintans.ezSQL.db.Relation;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.Condition;
import com.github.quintans.ezSQL.dml.Definition;
import com.github.quintans.ezSQL.dml.Delete;
import com.github.quintans.ezSQL.dml.Insert;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.dml.Update;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.toolkit.io.BinStore;
import com.github.quintans.ezSQL.toolkit.io.TextStore;
import com.github.quintans.ezSQL.transformers.BeanProperty;
import com.github.quintans.jdbc.JdbcSession;
import com.github.quintans.jdbc.SimpleJdbc;
import com.github.quintans.jdbc.exceptions.PersistenceException;

public abstract class AbstractDb {
	private static Logger LOGGER = Logger.getLogger(AbstractDb.class);
	private Driver driver;
	// using the same simple jdbc will allow an efficient use of sql types, because they are cached by SimpleJdbc instance. see SimpleJdbc.columnTypeCache
	private JdbcSession jdbcSession = new DbJdbcSession(this);

	public AbstractDb() {
	}

	/**
	 * Obtem a ligação a usar para executar o SQL<br>
	 * Implementações deste método podem ser a simples devolução de uma Connection previamente<br>
	 * definida, ou obtenção de uma Connection definida na Thread por frameworks transacionais<br>
	 * <p>
	 * Spring ex: Connection conn = DataSourceUtils.getConnection(dataSource);
	 * </p>
	 * 
	 * @return
	 */
	protected abstract Connection connection();
	
    protected abstract void releaseConnection(Connection connection);
    
	public Connection getConnection() {
		Connection conn = connection();
		this.driver.prepareConnection(conn);
		return conn;
	}
	
    public void returnConnection(Connection connection) {
        releaseConnection(connection);
    }

	/**
	 * gets the value at the end of the association from the database, puts in the input bean and returns the value.
	 * This method relies in reflection 
	 * 
	 * @param bean the target bean
	 * @param association the mapping
	 * @return the bean instance returned from de data base
	 */
	@SuppressWarnings("unchecked")
    public <T> T loadAssociation(Object bean, Association association){
        Map<String, BeanProperty> mappings = BeanProperty.populateMapping(null, bean.getClass());
        // check if target property is set
        String targetAlias = association.getAlias();
        BeanProperty targetBp = mappings.get(targetAlias);
        Object value = null;
        try {
            value = targetBp.invokeReadMethod(bean);
            if(value != null){ // if value is set returns
                return (T) value;
            } else if(bean instanceof Updatable) {
                Set<String> changed = ((Updatable) bean).changed();
                if(changed != null && changed.contains(targetBp.getName())) {
                    return null;
                }
            }
            
            Class<?> klass = targetBp.getKlass();
            
            Relation[] relations = association.getRelations();
            List<Condition> restrictions = new ArrayList<Condition>();
            for(Relation relation : relations) {
                // from source FK
                String fkAlias = relation.getFrom().getColumn().getAlias();
                BeanProperty bp = mappings.get(fkAlias);
                value = bp.invokeReadMethod(bean);
                restrictions.add(relation.getTo().getColumn().is(Definition.raw(value)));
            }
    
            Query query = query(association.getTableTo()).all()
            .where(restrictions);
            if(Collection.class.isAssignableFrom(klass)){
                Class<?> genKlass = targetBp.getGenericClass();
                value = query.list(genKlass);
                if(Set.class.isAssignableFrom(klass)){
                    value = new LinkedHashSet<Object>((Collection<? extends Object>) value);
                } else if(List.class.isAssignableFrom(klass)){
                    value = new ArrayList<Object>((Collection<? extends Object>) value);
                }
            } else {
                value = query.select(klass);
            }
        
            targetBp.invokeWriteMethod(bean, value);
            
        } catch(Exception ex) {
            throw new PersistenceException("Unable to retrive association " + association + " for " + bean, ex);
        }
        
        return (T) value;
	}

	/**
	 * selct over a table.<br>
	 * If no columns are added, when executing a select or list all columns of the driving table will be added.
	 * 
	 * @param table
	 * @return
	 */
	public Query query(Table table) {
		return new Query(this, table);
	}

	public Query query(Query query) {
		return new Query(query);
	}

	public Driver getDriver() {
		return this.driver;
	}

	public void setDriver(Driver driver) {
		this.driver = driver;
	}

	public JdbcSession getJdbcSession() {
		return this.jdbcSession;
	}

	public Long fetchAutoNumberBefore(Column<? extends Number> column) {
		return fetchAutoNumber(column, false);
	}

	public Long fetchCurrentAutoNumberAfter(Column<? extends Number> column) {
		return fetchAutoNumber(column, true);
	}

	public Long fetchAutoNumber(Column<? extends Number> column, boolean after) {
		String sql = after ? this.driver.getCurrentAutoNumberQuery(column) : this.driver.getAutoNumberQuery(column);
		long now = 0;
		if(LOGGER.isDebugEnabled()) {
    		LOGGER.debug("SQL: " + sql);
    		now = System.nanoTime();
		}
		SimpleJdbc jdbc = new SimpleJdbc(jdbcSession);
		Long id = jdbc.queryForLong(sql, new LinkedHashMap<String, Object>());
		if(LOGGER.isDebugEnabled()) {
		    LOGGER.debug("executed in: " + (System.nanoTime() - now)/1e6 + "ms");
		}
		return id;
	}

	public Insert insert(Table table) {
		return new Insert(this, table);
	}

	public Update update(Table table) {
		return new Update(this, table);
	}

    public Delete delete(Table table) {
        return new Delete(this, table);
    }
	
    // converte os valores
    public Map<String, Object> transformParameters(Map<String, Object> parameters) {
        Map<String, Object> pars = new LinkedHashMap<String, Object>();
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            Object val = entry.getValue();
            val = transformParameter(val);
            pars.put(entry.getKey(), val);
        }

        return pars;
    }

    public Object[] transformParameters(Object... parameters) {
        if(parameters == null)
            return null;
        
        Object[] vals = new Object[parameters.length];
        int i = 0;
        for(Object parameter : parameters) {
            vals[i++] = transformParameter(parameter);
        }
        
        return vals;
    }
    
    public Object transformParameter(Object parameter) {
        Object val = parameter;
        if (val instanceof NullSql) {
        	val = driver.fromNull((NullSql) val);
        } 
        else if (val instanceof Boolean) {
            val = driver.fromBoolean((Boolean) val);
        }
        else if (val instanceof Byte) {
            val = driver.fromTiny((Byte) val);
        }
        else if (val instanceof Short) {
            val = driver.fromShort((Short) val);
        }
        else if (val instanceof Integer) {
            val = driver.fromInteger((Integer) val);
        }
        else if (val instanceof Long) {
            val = driver.fromLong((Long) val);
        }
        else if (val instanceof Double) {
            val = driver.fromDecimal((Double) val);
        }
        else if (val instanceof BigDecimal) {
            val = driver.fromBigDecimal((BigDecimal) val);
        }
        else if (val instanceof String) {
            val = driver.fromString((String) val);
        }
        else if (val instanceof MyTime) {
            val = driver.fromTime((Date) val);
        }
        else if (val instanceof MyDate) {
            val = driver.fromDate((Date) val);
        }
        else if (val instanceof MyDateTime) {
            val = driver.fromDateTime((Date) val);
        }
        else if (val instanceof Date) {
            val = driver.fromTimestamp((Date) val);
        }
        else if (val instanceof TextStore) {
            TextStore txt = (TextStore) val;
            try {
                val = driver.fromText(txt.getInputStream(), (int) txt.getSize());
            } catch (IOException e) {
                throw new PersistenceException("Unable to get input stream from TextCache!", e);
            }
        }
        else if (val instanceof BinStore) {
            BinStore bin = (BinStore) val;
            try {
                val = driver.fromBin(bin.getInputStream(), (int) bin.getSize());
            } catch (IOException e) {
                throw new PersistenceException("Unable to get input stream from ByteCache!", e);
            }
        }
        else if (val instanceof char[]) {
            String txt = new String((char[]) val);
            InputStream is;
            try {
                is = IOUtils.toInputStream(txt, TextStore.DEFAULT_CHARSET);
                val = driver.fromText(is, txt.length());
            } catch (IOException e) {
                throw new PersistenceException("Unable to get input stream from String!", e);
            }
        }
        else if (val instanceof byte[]) {
            byte[] bin = (byte[]) val;
            val = driver.fromBin(new ByteArrayInputStream(bin), bin.length);
        }
        else {
            val = driver.fromUnknown(val);
        }

        return val;
    }

}
