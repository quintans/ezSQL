package com.github.quintans.ezSQL.dml;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.common.api.PostDeleter;
import com.github.quintans.ezSQL.common.api.PreDeleter;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Discriminator;
import com.github.quintans.ezSQL.db.PreDeleteTrigger;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.exceptions.OptimisticLockException;
import com.github.quintans.ezSQL.toolkit.utils.Result;
import com.github.quintans.jdbc.RawSql;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static com.github.quintans.ezSQL.dml.Definition.param;

public class Delete extends Dml<Delete> {
    private static final Logger LOG = Logger.getLogger(Delete.class);
    private static final String FQCN = Delete.class.getName();

    public Delete(AbstractDb db, Table table) {
        super(db, table);
    }

    @Override
    public Delete values(Object... values) {
        throw new UnsupportedOperationException("Method 'Delete.values' is not implemented");
    }

    /**
     * Builds the delete action considering only the key properties.<br>
     * Version column is ignored.
     *
     * @param bean
     * @return this
     */
    public Delete set(Object bean) {
        mapBean(bean, false);

        return this;
    }

    public int execute() {
        PreDeleteTrigger pre = getTable().getPreDeleteTrigger();
        if (pre != null) {
            pre.trigger(this);
        }

        return execute(LOG, FQCN, this.parameters);
    }

    public int[] batch() {
        PreDeleteTrigger pre = getTable().getPreDeleteTrigger();
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

    @Override
    public RawSql getSql() {
        if (this.rawSql == null) {
            String sql = driver().getSql(this);
            this.rawSql = RawSql.of(sql);
        }
        return this.rawSql;
    }

    /**
     * Remove the table row associated with the supplied bean.<br>
     * Version property must exist and must be non null, otherwise it will throw an OptimisticLockException.<br>
     * An OptimisticLockException is thrown if it fails to update.<br>
     * See also {@link #execute(Object) execute(Object)}.<br>
     *
     * @param bean
     */
    public void submit(Object bean) {
        if (!_execute(bean))
            throw new OptimisticLockException(OPTIMISTIC_LOCK_MSG);

        if (bean instanceof PostDeleter) {
            ((PostDeleter) bean).postDelete();
        }
    }

    /**
     * Remove the table row associated with the supplied bean.<br>
     * Version column is ignored if null.
     *
     * @param bean
     * @return success
     */
    public boolean execute(Object bean) {
        boolean result = _execute(bean);

        if (bean instanceof PostDeleter) {
            ((PostDeleter) bean).postDelete();
        }

        return result;
    }

    private boolean _execute(Object bean) {
        if (bean == null)
            throw new IllegalArgumentException("Cannot delete a null object.");

        if (bean instanceof PreDeleter) {
            ((PreDeleter) bean).preDelete();
        }

        mapBean(bean, true);

        // table discriminators have higher priority
        if (table.getDiscriminators() != null) {
            List<Condition> conditions = new ArrayList<Condition>(table.getDiscriminators().size());
            for (Discriminator disc : table.getDiscriminators()) {
                conditions.add(disc.getCondition());
            }
            this.where(conditions);
        }

        return this.execute() != 0;
    }

    private void mapBean(Object bean, boolean versioned) {
        this.parameters = new LinkedHashMap<>();
        this.values = new LinkedHashMap<>();

        List<Condition> conditions = null;
        if (bean.getClass() != this.lastBeanClass) {
            conditions = new ArrayList<>();
            this.condition = null;
            this.lastBeanClass = bean.getClass();
            this.rawSql = null;
        }

        for (Column<?> column : table.getColumns()) {
            if (column.isKey() || versioned && column.isVersion()) {
                Result<Object> result = db.findDeleteMapper(bean.getClass()).map(column, bean);
                if(result.isSuccess()) {
                    Object o = result.get();
                    String alias = column.getAlias();
                    if (column.isKey()) {
                        if (o == null)
                            throw new PersistenceException(String.format("Value for key property '%s' cannot be null.", alias));

                        if (conditions != null) {
                            conditions.add(column.is(param(alias)));
                        }
                        this.setParameter(column, o);
                    } else if (versioned && column.isVersion()) {
                        // if version is null ignores it
                        if (o != null) {
                            String as = "_" + alias + "_";
                            if (conditions != null) {
                                conditions.add(column.is(param(as)));
                            }
                            this.setParameter(as, o);
                        }
                    }
                }
            }
        }

        if (conditions != null) {
            this.where(conditions);
        }
    }

}
