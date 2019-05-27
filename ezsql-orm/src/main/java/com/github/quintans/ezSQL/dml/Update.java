package com.github.quintans.ezSQL.dml;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.common.api.PostUpdater;
import com.github.quintans.ezSQL.common.api.PreUpdater;
import com.github.quintans.ezSQL.common.api.Updatable;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Discriminator;
import com.github.quintans.ezSQL.db.PreUpdateTrigger;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.driver.EDml;
import com.github.quintans.ezSQL.exceptions.OptimisticLockException;
import com.github.quintans.ezSQL.toolkit.utils.Appender;
import com.github.quintans.ezSQL.toolkit.utils.Result;
import com.github.quintans.ezSQL.transformers.UpdateMapper;
import com.github.quintans.ezSQL.transformers.UpdateValue;
import com.github.quintans.jdbc.SimpleJdbc;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.function.Consumer;

import static com.github.quintans.ezSQL.dml.Definition.param;

public class Update extends CoreDSL {
  private static final Logger LOG = Logger.getLogger(Update.class);
  private final AbstractDb db;
  private final JdbcExecutor executor;

  public Update(AbstractDb db, Table table) {
    super(db.getDriver(), table);
    this.db = db;
    this.executor = new JdbcExecutor(driver, new SimpleJdbc(db.getJdbcSession()));
    this.values = new LinkedHashMap<>();
  }

  public AbstractDb getDb() {
    return db;
  }

  public Map<Column<?>, Function> getValues() {
    return this.values;
  }

  public Update set(Column<?> column) {
    _set(column, column.param());
    return this;
  }

  public Update set(Column<?> col, Function value) {
    _set(col, value);
    return this;
  }

  public <C> Update set(Column<C> col, C value) {
    _set(col, value);
    return this;
  }

  public <C> Update set(Column<C> col, Column<C> value) {
    _set(col, value);
    return this;
  }

  @SuppressWarnings("unchecked")
  public <C> C get(Column<C> col) {
    return (C) _get(col);
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

  public Update sets(Column<?>... columns) {
    _sets(columns);
    return this;
  }

  public Update values(Object... values) {
    _values(values);
    return this;
  }

  public int execute() {
    PreUpdateTrigger pre = getTable().getPreUpdateTrigger();
    if (pre != null) {
      pre.trigger(this);
    }
    return executor.execute(getRawSql(), this.parameters);
  }

  public int[] batch() {
    PreUpdateTrigger pre = getTable().getPreUpdateTrigger();
    if (pre != null) {
      pre.trigger(this);
    }
    return executor.batch(getRawSql(), this.parameters);
  }

  /**
   * Sends the batched commands to the database. This will not close the batch. For that use {@link #endBatch() endBatch}
   *
   * @return
   * @see #endBatch()
   */
  public int[] flushBatch() {
    return executor.flushBatch(getRawSql(), this.parameters);
  }

  /**
   * Closes batch freeing resources. Will also flush any pending dml commands.
   */
  public void endBatch() {
    executor.endBatch(getRawSql(), this.parameters);
  }

  @Override
  public String getSql() {
    return driver.getSql(this);
  }

  private String getTablePart() {
    Appender tablePart = new Appender();
    Table table = getTable();
    String alias = getTableAlias();
    tablePart.addAsOne(getDriver().tableName(table), " ", getDriver().tableAlias(alias));
    return tablePart.toString();
  }

  private String getColumnPart() {
    Appender columnPart = new Appender(", ");
    Map<Column<?>, Function> values = getValues();
    if (values != null) {
      for (Map.Entry<Column<?>, Function> entry : values.entrySet()) {
        column(columnPart, entry.getKey(), entry.getValue());
      }
    }
    return columnPart.toString();
  }

  private void column(Appender columnPart, Column<?> column, Function token) {
    columnPart.addAsOne(getTableAlias(), ".",
        getDriver().columnName(column),
        " = ", getDriver().translate(EDml.UPDATE, token));
  }

  private String getWherePart() {
    Appender wherePart = new Appender(" AND ");
    Condition criteria = getCondition();
    if (criteria != null) {
      wherePart.add(getDriver().translate(EDml.UPDATE, criteria));
    }
    return wherePart.toString();
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
      throw new OptimisticLockException();

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
      throw new PersistenceException("Field ID is undefined!");
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
    if (idVer.versionValue != null) {
      if (result > 0) {
        idVer.setter.accept(idVer.versionValue);
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
        UpdateMapper mapper = getDriver().findUpdateMapper(bean.getClass());
        Result<UpdateValue> result = mapper.map(driver, column, bean);
        if (result.isSuccess()) {
          UpdateValue updateValue = result.get();
          Object o = updateValue.getCurrent();
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
              idVer.versionValue = mapper.newVersion(o);
              idVer.setter = updateValue.getSetter();
              String as = "_" + alias + "_";
              if (conditions != null) {
                conditions.add(column.is(param(as)));
              }
              this.setParameter(as, o);

              this._set(column, idVer.versionValue);
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
    Consumer<Object> setter = null;
    Object versionValue = null;
  }

  public Update where(Condition... restrictions) {
    super._where(restrictions);
    return this;
  }

  public Update where(List<Condition> restrictions) {
    super._where(restrictions);
    return this;
  }
}
