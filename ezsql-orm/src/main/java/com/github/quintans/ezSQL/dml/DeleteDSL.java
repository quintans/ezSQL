package com.github.quintans.ezSQL.dml;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.toolkit.utils.Result;
import com.github.quintans.jdbc.exceptions.PersistenceException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static com.github.quintans.ezSQL.dml.Definition.param;

public class DeleteDSL<T extends DeleteDSL<T>> extends CoreDSL {

  public DeleteDSL(Driver driver, Table table) {
    super(driver, table);
  }

  @Override
  protected String computeSql() {
    return driver.getSql(this);
  }

  /**
   * Builds the delete action considering only the key properties.<br>
   * Version column is ignored.
   *
   * @param bean
   * @return this
   */
  @SuppressWarnings("unchecked")
  public T set(Object bean) {
    mapBean(bean, false);

    return (T) this;
  }

  protected void mapBean(Object bean, boolean versioned) {
    this.parameters = new LinkedHashMap<>();
    this.values = new LinkedHashMap<>();

    List<Condition> conditions = null;
    if (bean.getClass() != this.lastBeanClass) {
      conditions = new ArrayList<>();
      this.condition = null;
      this.lastBeanClass = bean.getClass();
      this.lastSql = null;
    }

    for (Column<?> column : table.getColumns()) {
      if (column.isKey() || versioned && column.isVersion()) {
        Result<Object> result = getDriver().findDeleteMapper(bean.getClass()).map(getDriver(), column, bean);
        if (result.isSuccess()) {
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

  @SuppressWarnings("unchecked")
  public T where(Condition... restrictions) {
    super.coreWhere(restrictions);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T where(List<Condition> restrictions) {
    super.coreWhere(restrictions);
    return (T) this;
  }

}
