package pt.quintans.ezSQL.dml;

import static pt.quintans.ezSQL.dml.Definition.param;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import pt.quintans.ezSQL.AbstractDb;
import pt.quintans.ezSQL.common.api.PostDeleter;
import pt.quintans.ezSQL.common.api.PreDeleter;
import pt.quintans.ezSQL.db.Column;
import pt.quintans.ezSQL.db.Discriminator;
import pt.quintans.ezSQL.db.PreDeleteTrigger;
import pt.quintans.ezSQL.db.Table;
import pt.quintans.ezSQL.exceptions.OptimisticLockException;
import pt.quintans.ezSQL.exceptions.PersistenceException;
import pt.quintans.ezSQL.sql.RawSql;
import pt.quintans.ezSQL.transformers.BeanProperty;

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
     * 
     * @param bean
     */
    public Delete set(Object bean) {
        mapBean(bean, false);
        
        return this;
    }

    public int execute() {
        PreDeleteTrigger pre = getTable().getPreDeleteTrigger();
        if(pre != null) {
            pre.trigger(this);
        }
        
        return execute(LOG, FQCN, this.parameters);
    }

    public int[] batch(){
        PreDeleteTrigger pre = getTable().getPreDeleteTrigger();
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
    
    @Override
    public RawSql getSql() {
        if (this.rawSql == null) {
            // if the discriminator conditions have not yet been processed, apply them now
            if (this.discriminatorConditions != null && this.condition == null) {
                where(this.discriminatorConditions);
            }

            String sql = driver().getSql(this);
            this.rawSql = getSimpleJdbc().toRawSql(sql);
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
        
        if(bean instanceof PostDeleter) {
            ((PostDeleter) bean).postDelete();
        }
    }
    
    /**
     * Remove the table row associated with the supplied bean.<br>
     * Version column is ignored if null.
     * 
     * @param bean
     */
    public boolean execute(Object bean) {
        boolean result = _execute(bean);
        
        if(bean instanceof PostDeleter) {
            ((PostDeleter) bean).postDelete();
        }
        
        return result;
    }

    private boolean _execute(Object bean) {
        if (bean == null)
            throw new IllegalArgumentException("Cannot delete a null object.");

        if(bean instanceof PreDeleter) {
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

    private void mapBean(Object bean, boolean versioned){
        this.parameters = new LinkedHashMap<String, Object>();
        this.values = new LinkedHashMap<Column<?>, Function>();
                
        Map<String, BeanProperty> mappings = null;
        List<Condition> conditions = null;
        if (bean.getClass() == this.lastBeanClass) {
            mappings = this.lastMappings;
        }
        else {
            mappings = BeanProperty.populateMapping(null, bean.getClass());
            conditions = new ArrayList<Condition>();
            this.condition = null;
            this.lastMappings = mappings;
            this.lastBeanClass = bean.getClass();
            this.rawSql = null;
        }

        for (Column<?> column : table.getColumns()) {
            if(column.isKey() || versioned && column.isVersion()) {
                String alias = column.getAlias();
                BeanProperty bp = mappings.get(alias);
                if (bp != null) {
                    Object o = null;
                    try {
                        o = bp.invokeReadMethod(bean);
                    } catch (Exception e) {
                        throw new PersistenceException("Unable to read from " + bean.getClass().getSimpleName() + "." + bp.getReadMethod().getName(), e);
                    }
    
                    if (column.isKey()) {
                        if (o == null)
                            throw new PersistenceException(String.format("Value for key property '%s' cannot be null.", alias));
    
                        if(conditions != null) {
                            conditions.add(column.is(param(alias)));
                        }
                        this.setParameter(column, o);
                    } else if (versioned && column.isVersion()) {
                        // if version is null ignores it
                        if (o != null) {
                            String as = "_" + alias + "_";
                            if(conditions != null) {
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
