package com.github.quintans.ezSQL.dml;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.common.api.PostUpdater;
import com.github.quintans.ezSQL.common.api.PreUpdater;
import com.github.quintans.ezSQL.common.api.Updatable;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Discriminator;
import com.github.quintans.ezSQL.db.PreUpdateTrigger;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.exceptions.OptimisticLockException;
import com.github.quintans.ezSQL.toolkit.utils.Misc;
import com.github.quintans.jdbc.RawSql;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import org.apache.log4j.Logger;

import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import static com.github.quintans.ezSQL.dml.Definition.param;

public class Update extends Dml<Update> {
    private static final Logger LOG = Logger.getLogger(Update.class);
    private static final String FQCN = Update.class.getName();

    public Update(AbstractDb db, Table table) {
        super(db, table);
        this.values = new LinkedHashMap<Column<?>, Function>();
    }

    public Update set(Column<?> column) {
        _set(column, column.param());
        return this;
    }

    public Update set(Column<?> col, Function value) {
        return _set(col, value);
    }

    public <C> Update set(Column<C> col, C value) {
        return _set(col, value);
    }

    public <C> Update set(Column<C> col, Column<C> value) {
        return _set(col, value);
    }

    public <C> Update with(Column<C> c, C value) {
        setParameter(c, value);
        return this;
    }

    public Update with(String name, Object value) {
        setParameter(name, value);
        return this;
    }

    /**
     * Sets all the columns of the table to matching bean property.<br>
     * Version column is ignored.
     *
     * @param bean The bean to match
     * @return this
     */
    public Update set(Object bean) {
        mapBean(bean, false);

        if (bean instanceof Updatable) {
            ((Updatable) bean).clear();
        }

        return this;
    }

    public int execute() {
        PreUpdateTrigger pre = getTable().getPreUpdateTrigger();
        if (pre != null) {
            pre.trigger(this);
        }
        return execute(LOG, FQCN, this.parameters);
    }

    public int[] batch() {
        PreUpdateTrigger pre = getTable().getPreUpdateTrigger();
        if (pre != null) {
            pre.trigger(this);
        }
        return batch(LOG, FQCN, this.parameters);
    }

    /**
     * Sends the batched commands to the database. This will not close the batch. For that use {@link #endBatch() endBatch}
     *
     * @return
     * @see #endBatch()
     */
    public int[] flushBatch() {
        return flushBatch(LOG, FQCN);
    }

    /**
     * Closes batch freeing resources. Will also flush any pending dml commands.
     */
    public void endBatch() {
        endBatch(LOG, FQCN);
    }

    @Override
    public RawSql getSql() {
        if (this.rawSql == null) {
            String sql = driver().getSql(this);
            this.rawSql = getSimpleJdbc().toRawSql(sql);
        }
        return this.rawSql;
    }

    /**
     * Sets all the columns of the table to matching bean property.<br>
     * Version property must exist and must be non null, otherwise it will throw an OptimisticLockException.<br>
     * An OptimisticLockException is thrown if it fails to update.<br>
     * See also {@link #execute(Object) execute(Object)}.<br>
     *
     * @param bean the bean to update. Cannot be null.
     */
    public void submit(Object bean) {
        if (!_execute(bean))
            throw new OptimisticLockException(OPTIMISTIC_LOCK_MSG);

        if (bean instanceof PostUpdater) {
            ((PostUpdater) bean).postUpdate();
        }

        if (bean instanceof Updatable) {
            ((Updatable) bean).clear();
        }

    }

    /**
     * Sets all the columns of the table to matching bean property.<br>
     * Version column is ignored if null.
     *
     * @param bean the bean to update. Cannot be null.
     * @return true if it was successful.
     */
    public boolean execute(Object bean) {
        boolean result = _execute(bean);

        if (bean instanceof PostUpdater) {
            ((PostUpdater) bean).postUpdate();
        }

        if (bean instanceof Updatable) {
            ((Updatable) bean).clear();
        }

        return result;
    }

    private boolean _execute(Object bean) {
        if (bean == null)
            throw new IllegalArgumentException("Cannot update a null object.");

        if (bean instanceof PreUpdater) {
            ((PreUpdater) bean).preUpdate();
        }

        IdVer idVer = mapBean(bean, true);

        if (idVer.noId) {
            throw new PersistenceException(ID_UNDEFINED_MSG);
        }

        /*
         * table discriminators have higher priority.
         * no updates can be made to this columns and the discriminator will be enforced.
         *
         */
        if (table.getDiscriminators() != null) {
            List<Condition> conditions = new ArrayList<Condition>(table.getDiscriminators().size());
            for (Discriminator disc : table.getDiscriminators()) {
                conditions.add(disc.getCondition());
                this.values.remove(disc.getColumn());
            }
            this.where(conditions);
        }

        int result = this.execute();
        if (idVer.versionBeanProperty != null) {
            if (result > 0) {
                try {
                    idVer.versionBeanProperty.getWriteMethod().invoke(bean, idVer.versionValue);
                } catch (Exception e) {
                    throw new PersistenceException(VERSION_SET_MSG, e);
                }
            }
        }

        return result != 0;
    }

    private IdVer mapBean(Object bean, boolean versioned) {
        this.parameters = new LinkedHashMap<>();
        this.values = new LinkedHashMap<>();

        List<Condition> conditions = null;
        if (bean.getClass() != this.lastBeanClass) {
            conditions = new ArrayList<Condition>();
            this.condition = null;
            this.lastBeanClass = bean.getClass();
            this.rawSql = null;
        }

        IdVer idVer = new IdVer();

        Set<String> changed = null;
        if (bean instanceof Updatable) {
            changed = ((Updatable) bean).changed();
        }

        for (Column<?> column : table.getColumns()) {
            String alias = column.getAlias();
            if (changed == null || column.isKey() || column.isVersion() || changed.contains(alias)) {
                PropertyDescriptor pd = Misc.getPropertyDescriptor(bean.getClass(), alias);
                if (pd != null) {
                    Object o;
                    try {
                        o = pd.getReadMethod().invoke(bean);
                    } catch (Exception e) {
                        throw new PersistenceException("Unable to read from " + bean.getClass().getSimpleName() + "." + pd.getReadMethod().getName(), e);
                    }

                    if (column.isKey()) {
                        if (o == null)
                            throw new PersistenceException(String.format("Value for key property '%s' cannot be null.", alias));

                        if (conditions != null) {
                            conditions.add(column.is(param(alias)));
                        }
                        this.setParameter(alias, o);

                        idVer.noId = false;
                    } else if (versioned && column.isVersion()) {
                        // if version is null ignores it
                        if (o != null) {
                            // version increment
                            if (Long.class.isAssignableFrom(pd.getPropertyType())) {
                                idVer.versionValue = (Long) o + 1L;
                            } else {
                                idVer.versionValue = (Integer) o + 1;
                            }

                            String as = "_" + alias + "_";
                            if (conditions != null) {
                                conditions.add(column.is(param(as)));
                            }
                            this.setParameter(as, o);

                            this._set(column, idVer.versionValue);

                            idVer.versionBeanProperty = pd;
                        }
                    } else {
                        this._set(column, o);
                    }
                }
            }
        }

        if (conditions != null) {
            where(conditions);
        }

        return idVer;
    }

    private static class IdVer {
        boolean noId = true;
        PropertyDescriptor versionBeanProperty = null;
        Object versionValue = null;
    }
}
