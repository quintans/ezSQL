package com.github.quintans.ezSQL.dml;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.common.api.PostDeleter;
import com.github.quintans.ezSQL.common.api.PreDeleter;
import com.github.quintans.ezSQL.db.Discriminator;
import com.github.quintans.ezSQL.db.PreDeleteTrigger;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.exceptions.OptimisticLockException;
import com.github.quintans.jdbc.SimpleJdbc;

import java.util.ArrayList;
import java.util.List;

public class Delete extends DeleteDSL<Delete> {
  private AbstractDb db;
  private final JdbcExecutor executor;

  public Delete(AbstractDb db, Table table) {
    super(db.getDriver(), table);
    this.db = db;
    this.executor = new JdbcExecutor(driver, new SimpleJdbc(db.getJdbcSession()));
  }

  public AbstractDb getDb() {
    return db;
  }

  public int execute() {
    PreDeleteTrigger pre = getTable().getPreDeleteTrigger();
    if (pre != null) {
      pre.trigger(this);
    }

    return executor.execute(getSql(), this.parameters);
  }

  public int[] batch() {
    PreDeleteTrigger pre = getTable().getPreDeleteTrigger();
    if (pre != null) {
      pre.trigger(this);
    }

    return executor.batch(getSql(), this.parameters);
  }

  public int[] flushBatch() {
    return executor.flushBatch(getSql(), this.parameters);
  }

  public void endBatch() {
    executor.endBatch(getSql(), this.parameters);
  }

  /**
   * Remove the table row associated with the supplied bean.<br>
   * Version property must exist and must be non null, otherwise it will throw an OptimisticLockException.<br>
   * An OptimisticLockException is thrown if it fails to update.<br>
   * See also {@link #execute(Object) execute(Object)}.<br>
   *
   * @param bean bean
   */
  public void submit(Object bean) {
    if (!myExecute(bean))
      throw new OptimisticLockException();

    if (bean instanceof PostDeleter) {
      ((PostDeleter) bean).postDelete();
    }
  }

  /**
   * Remove the table row associated with the supplied bean.<br>
   * Version column is ignored if null.
   *
   * @param bean bean
   * @return success
   */
  public boolean execute(Object bean) {
    boolean result = myExecute(bean);

    if (bean instanceof PostDeleter) {
      ((PostDeleter) bean).postDelete();
    }

    return result;
  }

  private boolean myExecute(Object bean) {
    if (bean == null)
      throw new IllegalArgumentException("Cannot delete a null object.");

    if (bean instanceof PreDeleter) {
      ((PreDeleter) bean).preDelete();
    }

    mapBean(bean, true);

    // table discriminators have higher priority
    if (table.getDiscriminators() != null) {
      List<Condition> conditions = new ArrayList<>(table.getDiscriminators().size());
      for (Discriminator disc : table.getDiscriminators()) {
        conditions.add(disc.getCondition());
      }
      this.where(conditions);
    }

    return this.execute() != 0;
  }
}
