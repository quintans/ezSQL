package com.github.quintans.ezSQL.dml;

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
import com.github.quintans.ezSQL.driver.EDml;
import com.github.quintans.ezSQL.toolkit.reflection.FieldUtils;
import com.github.quintans.ezSQL.toolkit.reflection.TypedField;
import com.github.quintans.ezSQL.toolkit.utils.Appender;
import com.github.quintans.ezSQL.toolkit.utils.Tuple2;
import com.github.quintans.ezSQL.transformers.InsertMapper;
import com.github.quintans.jdbc.ColumnType;
import com.github.quintans.jdbc.RawSql;
import com.github.quintans.jdbc.SimpleJdbc;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import org.apache.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class Insert extends InsertDSL<Insert> {
  private static final Logger LOG = Logger.getLogger(Insert.class);
  private final AbstractDb db;

  private boolean returnKey = true;
  private boolean previousHasAllKeyValues;
  private final JdbcExecutor executor;

  public Insert(AbstractDb db, Table table) {
    super(db.getDriver(), table);
    this.db = db;
    this.executor = new JdbcExecutor(driver, new SimpleJdbc(db.getJdbcSession()));
    this.values = new LinkedHashMap<>();

    List<Discriminator> discriminators = table.getDiscriminators();
    if (discriminators != null) {
      for (Discriminator discriminator : discriminators) {
        coreSet(discriminator.getColumn(), discriminator.getValue());
      }
    }
  }

  public AbstractDb getDb() {
    return db;
  }

  public Insert retrieveKeys(boolean returnKey) {
    this.returnKey = returnKey;
    return this;
  }

  public int getPending() {
    return executor.getPending();
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

  public Map<Column<?>, Object> execute() {
    PreInsertTrigger pre = getTable().getPreInsertTrigger();
    if (pre != null) {
      pre.trigger(this);
    }

    boolean hasAllKeyValues = hasAllKeyValues();
    if (previousHasAllKeyValues != hasAllKeyValues) {
      this.lastSql = null;
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
          lastKey = executor.fetchAutoNumberBefore(singleKeyColumn);
          this.coreSet(singleKeyColumn, lastKey);
          kmap.put(singleKeyColumn, lastKey);
        }
        executor.update(getSql(), this.parameters);
        break;

      case RETURNING:
        Column<?>[] columns = null;
        ColumnType[] keyColumns = null;
        Set<Column<?>> kcs = null;
        if (kmap != null) {
          // nome das colunas chave, para obtensão das chaves geradas
          kcs = getTable().getKeyColumns();
          columns = new Column[kcs.size()];
          keyColumns = new ColumnType[kcs.size()];
          int i = 0;
          for (Column<?> c : kcs) {
            columns[i] = c;
            keyColumns[i++] = new ColumnType(driver.columnName(c), c.getKeyType());
          }
        }

        Object[] keys = executor.insert(getSql(), keyColumns, this.parameters);
        if (kmap != null) {
          for (int i = 0; i < columns.length; i++) {
            kmap.put(columns[i], keys[i]);
          }
        }
        break;

      case AFTER:
        executor.update(getSql(), this.parameters);
        if (kmap != null && singleKeyColumn != null) {
          lastKey = executor.fetchAutoNumberBefore(singleKeyColumn);
          kmap.put(singleKeyColumn, lastKey);
        }

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
    return executor.batch(getSql(), this.parameters);
  }

  /**
   * Sends the batched commands to the database. This will not close the batch. For that use {@link #endBatch() endBatch}
   *
   * @return
   * @see #endBatch()
   */
  public int[] flushBatch() {
    return executor.flushBatch(getSql(), this.parameters);
  }

  /**
   * Closes batch freeing resources. Will also flush any pending dml commands.
   */
  public void endBatch() {
    executor.endBatch(getSql(), this.parameters);
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
        this.coreSet(disc.getColumn(), disc.getValue());
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

}
