package com.github.quintans.ezSQL.dml;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.common.api.PostUpdater;
import com.github.quintans.ezSQL.common.api.PreUpdater;
import com.github.quintans.ezSQL.common.api.Updatable;
import com.github.quintans.ezSQL.db.Discriminator;
import com.github.quintans.ezSQL.db.PreUpdateTrigger;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.exceptions.OptimisticLockException;
import com.github.quintans.jdbc.SimpleJdbc;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class Update extends UpdateDSL<Update> {
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

  public int execute() {
    PreUpdateTrigger pre = getTable().getPreUpdateTrigger();
    if (pre != null) {
      pre.trigger(this);
    }
    return executor.execute(getSql(), this.parameters);
  }

  public int[] batch() {
    PreUpdateTrigger pre = getTable().getPreUpdateTrigger();
    if (pre != null) {
      pre.trigger(this);
    }
    return executor.batch(getSql(), this.parameters);
  }

  /**
   * Sends the batched commands to the database. This will not close the batch. For that use {@link #endBatch() endBatch}
   *
   * @return affected rows per batched statement
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
   * Sets all the columns of the table to matching bean property.<br>
   * Version property must exist and must be non null, otherwise it will throw an OptimisticLockException.<br>
   * An OptimisticLockException is thrown if it fails to update.<br>
   * See also {@link #execute(Object) execute(Object)}.<br>
   *
   * @param bean the bean to update. Cannot be null.
   */
  public void submit(Object bean) {
    if (!myExecute(bean))
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
    boolean result = myExecute(bean);

    if (bean instanceof PostUpdater) {
      ((PostUpdater) bean).postUpdate();
    }

    if (bean instanceof Updatable) {
      ((Updatable) bean).clear();
    }

    return result;
  }

  private boolean myExecute(Object bean) {
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

}
