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

public class Insert extends CoreDSL {
  private static final Logger LOG = Logger.getLogger(Insert.class);
  private final AbstractDb db;

  private boolean returnKey = true;
  private boolean previousHasAllKeyValues;
  private boolean hasKeyValue;
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

  public Insert set(Column<?> col, Function value) {
    return innerSet(col, value);
  }

  public <C> Insert set(Column<C> col, C value) {
    return innerSet(col, value);
  }

  @SuppressWarnings("unchecked")
  public <C> C get(Column<C> col) {
    return (C) coreGet(col);
  }

  public <C> Insert with(Column<C> c, C value) {
    setParameter(c, value);
    return this;
  }

  public Insert with(String name, Object value) {
    setParameter(name, value);
    return this;
  }

  protected Insert innerSet(Column<?> col, Object value) {
    super.coreSet(col, value);
    if (table.getSingleKeyColumn() != null && col.isKey()) {
      this.hasKeyValue = (value != null);
    }
    return this;
  }

  public Insert sets(Column<?>... columns) {
    coreSets(columns);
    return this;
  }

  public Insert values(Object... values) {
    coreValues(values);
    return this;
  }

  public Map<Column<?>, Function> getValues() {
    return this.values;
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
  public String computeSql() {
    return driver.getSql(this);
  }

  private String getTablePart() {
    Table table = getTable();
    return getDriver().tableName(table);
  }

  private Tuple2<Appender, Appender> column() {
    Appender columnPart = new Appender(", ");
    Appender valuePart = new Appender(", ");

    Map<Column<?>, Function> values = null;
    values = getValues();
    Map<String, Object> parameters = getParameters();
    if (values != null) {
      for (Entry<Column<?>, Function> entry : values.entrySet()) {
        Column<?> column = entry.getKey();
        Function token = entry.getValue();
        // only includes null keys if IgnoreNullKeys is false
        if (column.isKey() && getDriver().ignoreNullKeys() && EFunction.PARAM.equals(token.getOperator())) {
          // ignore null keys
          Object param = parameters.get(token.getValue());
          if (param == null || param instanceof NullSql) {
            token = null;
          }
        }

        if (token != null) {
          String val = getDriver().translate(EDml.INSERT, token);
          if (val != null) {
            columnPart.add(getDriver().columnName(column));
            valuePart.add(val);
          }
        }
      }
    }
    return new Tuple2<>(columnPart, valuePart);
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

  private void mapObject(Object object, boolean versioned) {
    this.parameters = new LinkedHashMap<>();
    this.values = new LinkedHashMap<>();

    if (object.getClass() != this.lastBeanClass) {
      this.lastBeanClass = object.getClass();
      this.lastSql = null;
    }

    Set<String> changed = null;
    if (object instanceof Updatable) {
      changed = ((Updatable) object).changed();
    }

    InsertMapper insertMapper = getDriver().findInsertMapper(object.getClass());
    for (Column<?> column : table.getColumns()) {
      String alias = column.getAlias();
      if (changed == null || column.isKey() || column.isVersion() || changed.contains(alias)) {
        insertMapper.map(getDriver(), column, object, versioned)
            .onSuccess(o -> {
              if (versioned && column.isVersion() && o == null) {
                throw new PersistenceException("Undefined version for " +
                    object.getClass().getSimpleName() + "." + alias);
              }
              this.coreSet(column, o);
            });
      }
    }
  }

}
