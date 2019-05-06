package com.github.quintans.ezSQL.dml;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.common.api.PostInserter;
import com.github.quintans.ezSQL.common.api.PreInserter;
import com.github.quintans.ezSQL.common.api.Updatable;
import com.github.quintans.ezSQL.db.*;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.toolkit.reflection.FieldUtils;
import com.github.quintans.ezSQL.toolkit.reflection.TypedField;
import com.github.quintans.ezSQL.transformers.InsertMapper;
import com.github.quintans.jdbc.RawSql;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import org.apache.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class Insert extends DmlCore<Insert> {
    private static final Logger LOG = Logger.getLogger(Insert.class);
    private static final String FQCN = Insert.class.getName();

    private boolean returnKey = true;
    private boolean previousHasAllKeyValues;
    private boolean hasKeyValue;

    public Insert(AbstractDb db, Table table) {
        super(db, table);
        this.values = new LinkedHashMap<>();

        List<Discriminator> discriminators = table.getDiscriminators();
        if (discriminators != null) {
            for (Discriminator discriminator : discriminators) {
                _set(discriminator.getColumn(), discriminator.getValue());
            }
        }
    }

    public Insert retrieveKeys(boolean returnKey) {
        this.returnKey = returnKey;
        return this;
    }

    public Insert set(Column<?> col, Function value) {
        return _set(col, value);
    }

    public <C> Insert set(Column<C> col, C value) {
        return _set(col, value);
    }

    @SuppressWarnings("unchecked")
    public <C> C get(Column<C> col) {
        return (C) _get(col);
    }

    public <C> Insert with(Column<C> c, C value) {
        setParameter(c, value);
        return this;
    }

    public Insert with(String name, Object value) {
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
        if (table.getSingleKeyColumn() != null) {
            return hasKeyValue;
        } else {
            for (Column<?> col : table.getKeyColumns()) {
                Function val = values.get(col);
                if (val == null || val.getValue() instanceof NullSql) {
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * Loads sets all the columns of the table to matching bean property
     *
     * @param bean The bean to match
     * @return this
     */
    public Insert set(Object bean) {
        mapObject(bean, false);

        if (bean instanceof Updatable) {
            ((Updatable) bean).clear();
        }

        return this;
    }

    @Override
    public RawSql getSql() {
        if (this.rawSql == null) {
            String sql = driver().getSql(this);
            this.rawSql = RawSql.of(sql);
        }
        return this.rawSql;
    }

    @SuppressWarnings("unchecked")
    public Map<Column<?>, Object> execute() {
        PreInsertTrigger pre = getTable().getPreInsertTrigger();
        if (pre != null) {
            pre.trigger(this);
        }

        boolean hasAllKeyValues = hasAllKeyValues();
        if (previousHasAllKeyValues != hasAllKeyValues) {
            this.rawSql = null;
        }
        previousHasAllKeyValues = hasAllKeyValues;

        Long lastKey;
        Map<String, Object> params;
        Driver driver = db.getDriver();
        AutoKeyStrategy strategy = driver.getAutoKeyStrategy();
        Column<? extends Number> singleKeyColumn = table.getSingleKeyColumn();
        RawSql cachedSql = null;
        long now;
        Map<Column<?>, Object> kmap = null;
        if (this.returnKey && !hasAllKeyValues) {
            kmap = new LinkedHashMap<>();
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

    public int[] batch() {
        PreInsertTrigger pre = getTable().getPreInsertTrigger();
        if (pre != null) {
            pre.trigger(this);
        }
        return batch(LOG, FQCN, this.parameters);
    }

    public int[] flushBatch() {
        return flushBatch(LOG, FQCN);
    }

    public void endBatch() {
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

        mapObject(bean, true);

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
                TypedField tf = FieldUtils.getBeanTypedField(bean.getClass(), col.getAlias());
                if (tf != null) {
                    try {
                        Class<?> clazz = tf.getPropertyType();
                        if (val instanceof Number) {
                            if (Long.class.isAssignableFrom(clazz)) {
                                val = ((Number) val).longValue();
                            } else {
                                val = ((Number) val).intValue();
                            }
                        }

                        tf.set(bean, val);
                    } catch (Exception e) {
                        throw new PersistenceException("Unable to write to " + bean.getClass().getSimpleName() + "." + col.getAlias(), e);
                    }
                }
            }
        }

        if (bean instanceof PostInserter) {
            ((PostInserter) bean).postInsert();
        }

        if (bean instanceof Updatable) {
            ((Updatable) bean).clear();
        }

        return keys;
    }

    private void mapObject(Object object, boolean versioned) {
        this.parameters = new LinkedHashMap<>();
        this.values = new LinkedHashMap<>();

        if (object.getClass() != this.lastBeanClass) {
            this.lastBeanClass = object.getClass();
            this.rawSql = null;
        }

        Set<String> changed = null;
        if (object instanceof Updatable) {
            changed = ((Updatable) object).changed();
        }

        InsertMapper insertMapper = db.findInsertMapper(object.getClass());
        for (Column<?> column : table.getColumns()) {
            String alias = column.getAlias();
            if (changed == null || column.isKey() || column.isVersion() || changed.contains(alias)) {
                insertMapper.map(column, object, versioned)
                        .onSuccess(o -> {
                            if (versioned && column.isVersion() && o == null) {
                                throw new PersistenceException("Undefined version for " +
                                        object.getClass().getSimpleName() + "." + alias);
                            }
                            this._set(column, o);
                        });
            }
        }
    }

}
