package com.github.quintans.ezSQL.dml;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.common.api.PostInserter;
import com.github.quintans.ezSQL.common.api.PreInserter;
import com.github.quintans.ezSQL.common.api.Updatable;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Discriminator;
import com.github.quintans.ezSQL.db.NullSql;
import com.github.quintans.ezSQL.db.PreInsertTrigger;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.transformers.BeanProperty;
import com.github.quintans.jdbc.RawSql;
import com.github.quintans.jdbc.exceptions.PersistenceException;

public class Insert extends DmlCore<Insert> {
    private static final Logger LOG = Logger.getLogger(Insert.class);
    private static final String FQCN = Insert.class.getName();

    private boolean returnKey = true;
    private boolean previousHasAllKeyValues;
    private boolean hasKeyValue;

    public Insert(AbstractDb db, Table table) {
        super(db, table);
        this.values = new LinkedHashMap<Column<?>, Function>();

        List<Discriminator> discriminators = table.getDiscriminators();
        if (discriminators != null) {
            for (Discriminator discriminator : discriminators) {
                _set(discriminator.getColumn(), discriminator.getValue());
            }
        }
    }

    public Insert retriveKeys(boolean returnKey) {
        this.returnKey = returnKey;
        return this;
    }

    public Insert set(Column<?> col, Function value) {
        return _set(col, value);
    }
    
    public <C> Insert set(Column<C> col, C value) {
        return _set(col, value);
    }
    
    public <C> Insert with(Column<C> c, C value){
        setParameter(c, value);
        return this;
    }
    
    public Insert with(String name, Object value){
        setParameter(name, value);
        return this;
    }    
    
    @Override
    protected Insert _set(Column<?> col, Object value) {
        super._set(col, value);
        if (table.getSingleKeyColumn() != null && col.isKey()) {
            this.hasKeyValue = (value != null);
        }
        return this;
    }
    
    private boolean hasAllKeyValues() {
        if(table.getSingleKeyColumn() != null) {
            return hasKeyValue;
        } else {
            for(Column<?> col : table.getKeyColumns()){
                Function val = values.get(col);
                if(val == null || val.getValue() instanceof NullSql) {
                    return false;
                }
            }
            
            return true;
        }
    }

    /**
     * Loads sets all the columns of the table to matching bean property
     * 
     * @param bean
     *            The bean to match
     * @return this
     */
    public Insert set(Object bean) {
        mapBean(bean, false);

        if(bean instanceof Updatable) {
            ((Updatable) bean).clear();
        }

        return this;
    }

    @Override
    public RawSql getSql() {
        if (this.rawSql == null) {
            String sql = driver().getSql(this);
            this.rawSql = getSimpleJdbc().toRawSql(sql);
        }
        return this.rawSql;
    }

    @SuppressWarnings("unchecked")
    public Map<Column<?>, Object> execute() {
        PreInsertTrigger pre = getTable().getPreInsertTrigger();
        if(pre != null) {
            pre.trigger(this);
        }
        
        boolean hasAllKeyValues = hasAllKeyValues();
        if(previousHasAllKeyValues != hasAllKeyValues) {
            this.rawSql = null;
        }
        previousHasAllKeyValues = hasAllKeyValues;

        Long lastKey;
        Map<String, Object> params;
        Driver driver = db.getDriver();
        AutoKeyStrategy strategy = driver.getAutoKeyStrategy();
        Column<? extends Number> singleKeyColumn = (Column<? extends Number>) table.getSingleKeyColumn();
        RawSql cachedSql = null;
        long now;
        Map<Column<?>, Object> kmap = null;
        if (this.returnKey && !hasAllKeyValues) {
            kmap = new LinkedHashMap<Column<?>, Object>();
        }

        switch (strategy) {
            case BEFORE:
                if (kmap != null && singleKeyColumn != null) {
                    lastKey = getDb().fetchAutoNumberBefore(singleKeyColumn);
                    this._set(singleKeyColumn, lastKey);
                    kmap.put(singleKeyColumn, lastKey);
                }
                cachedSql = this.getSql();
                debugSQL(LOG, FQCN, cachedSql.getOriginalSql());
                params = db.transformParameters(this.parameters);
                now = System.nanoTime();
                getSimpleJdbc().update(cachedSql.getSql(), cachedSql.buildValues(params));
                debugTime(LOG, FQCN, now);
                break;

            case RETURNING:
                Column<?>[] columns = null;
                String[] keyColumns = null;
                Set<Column<?>> kcs = null;
                if (kmap != null) {
                    // nome das colunas chave, para obtensão das chaves geradas
                    kcs = getTable().getKeyColumns();
                    columns = new Column[kcs.size()];
                    keyColumns = new String[kcs.size()];
                    int i = 0;
                    for (Column<?> c : kcs) {
                        columns[i] = c;
                        keyColumns[i++] = driver.columnName(c);
                    }
                }

                cachedSql = this.getSql();
                debugSQL(LOG, FQCN, cachedSql.getOriginalSql());

                params = db.transformParameters(this.parameters);

                now = System.nanoTime();
                Object[] keys = getSimpleJdbc().insert(cachedSql.getSql(), keyColumns, cachedSql.buildValues(params));
                debugTime(LOG, FQCN, now);
                if (kmap != null) {
                    for (int i = 0; i < columns.length; i++) {
                        kmap.put(columns[i], keys[i]);
                    }
                }
                break;

            case AFTER:
                cachedSql = this.getSql();
                debugSQL(LOG, FQCN, cachedSql.getOriginalSql());
                params = db.transformParameters(this.parameters);
                now = System.nanoTime();
                getSimpleJdbc().update(cachedSql.getSql(), cachedSql.buildValues(params));
                debugTime(LOG, FQCN, now);
                if (kmap != null && singleKeyColumn != null) {
                    lastKey = getDb().fetchAutoNumberBefore(singleKeyColumn);
                    kmap.put(singleKeyColumn, lastKey);
                }

                break;
            case NONE:
                break;
            default:
                break;
        }

        return kmap;
    }
    
    public int[] batch(){
        PreInsertTrigger pre = getTable().getPreInsertTrigger();
        if(pre != null) {
            pre.trigger(this);
        }
        return batch(LOG, FQCN, this.parameters);
    }

    public int[] flushBatch(){
        return flushBatch(LOG, FQCN);
    }
    
    public void endBatch(){
        endBatch(LOG, FQCN);
    }    

    /**
     * Insert a table row associated with the supplied bean.<br>
     * This method takes in consideration the <u>version</u> column type, if
     * exists.<b> This is a fast way of creating an Insert but for multiple
     * inserts it is not as efficient as values().execute().</b>
     * 
     * @param bean
     * @return
     */
    public Map<Column<?>, Object> submit(Object bean) {
        if (bean == null)
            return null;

        if (bean instanceof PreInserter) {
            ((PreInserter) bean).preInsert();
        }

        Map<String, BeanProperty> mappings = mapBean(bean, true);

        // table discriminators have higher priority - the is no way to override these values
        if (table.getDiscriminators() != null) {
            for (Discriminator disc : table.getDiscriminators()) {
                this._set(disc.getColumn(), disc.getValue());
            }
        }

        Map<Column<?>, Object> keys = this.execute();
        if (keys != null) {
            for (Entry<Column<?>, Object> entry : keys.entrySet()) {
                Column<?> col = entry.getKey();
                Object val = entry.getValue();
                // update bean key properties
                BeanProperty bp = mappings.get(col.getAlias());
                if (bp != null) {
                    try {
                        Class<?> clazz = bp.getKlass();
                        if (val instanceof Number) {
                            if (Long.class.isAssignableFrom(clazz)) {
                                val = ((Number) val).longValue();
                            } else {
                                val = ((Number) val).intValue();
                            }
                        }

                        bp.invokeWriteMethod(bean, val);
                    } catch (Exception e) {
                        throw new PersistenceException("Unable to write to " + bean.getClass().getSimpleName() + "." + bp.getWriteMethod().getName(), e);
                    }
                }
            }
        }

        if (bean instanceof PostInserter) {
            ((PostInserter) bean).postInsert();
        }

        if(bean instanceof Updatable) {
            ((Updatable) bean).clear();
        }
        
        return keys;
    }

    private Map<String, BeanProperty> mapBean(Object bean, boolean versioned) {
        this.parameters = new LinkedHashMap<String, Object>();
        this.values = new LinkedHashMap<Column<?>, Function>();
        
        Map<String, BeanProperty> mappings = null;
        if (bean.getClass() == this.lastBeanClass) {
            mappings = this.lastMappings;
        }
        else {
            mappings = BeanProperty.populateMapping(null, bean.getClass());
            this.lastMappings = mappings;
            this.lastBeanClass = bean.getClass();
            this.rawSql = null;
        }

        Set<String> changed = null;
        if(bean instanceof Updatable) {
            changed = ((Updatable) bean).changed();
        }
        
        boolean ignoreNullKeys = db.getDriver().ignoreNullKeys();
        for (Column<?> column : table.getColumns()) {
            String alias = column.getAlias();
            BeanProperty bp = null;
            if(changed == null || column.isKey() || column.isVersion() || changed.contains(alias)){
                bp = mappings.get(alias);
            }
            if (bp != null) {
                Object o = null;
                try {
                    o = bp.invokeReadMethod(bean);
                } catch (Exception e) {
                    throw new PersistenceException("Unable to read from " + bean.getClass().getSimpleName() + "." + bp.getReadMethod().getName(), e);
                }

                if(column.isKey()){
                    if(ignoreNullKeys) {
                        
                    }
                } else if (versioned && column.isVersion() && o == null) {
                    try {
                        if (Long.class.isAssignableFrom(bp.getKlass())) {
                            o = 1L;
                        } else {
                            o = 1;
                        }

                        bp.invokeWriteMethod(bean, o);
                    } catch (Exception e) {
                        throw new PersistenceException("Unable to write to " + bean.getClass().getSimpleName() + "." + bp.getWriteMethod().getName(), e);
                    }
                }
                this._set(column, o);
            }
        }
        
        return mappings;
    }
}
