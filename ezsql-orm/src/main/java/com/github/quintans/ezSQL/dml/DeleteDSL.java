package com.github.quintans.ezSQL.dml;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.translator.Translator;
import com.github.quintans.ezSQL.exception.OrmException;
import com.github.quintans.ezSQL.toolkit.utils.Result;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static com.github.quintans.ezSQL.dml.Definition.param;

public class DeleteDSL<T extends DeleteDSL<T>> extends CoreDSL {

  public DeleteDSL(Translator translator, Table table) {
    super(translator, table);
  }

  @Override
  protected String computeSql() {
    return translator.getSql(this);
  }

  /**
   * Builds the delete action considering only the key properties.<br>
   * Version column is ignored.
   *
   * @param bean Object to use to set the key properties
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
        Result<Object> result = getTranslator().findDeleteMapper(bean.getClass()).map(getTranslator(), column, bean);
        if (result.isSuccess()) {
          Object o = result.get();
          String alias = column.getAlias();
          if (column.isKey()) {
            if (o == null)
              throw new OrmException("Value for key property '%s' cannot be null.", alias);

            if (conditions != null) {
              conditions.add(column.eq(param(alias)));
            }
            this.setParameter(column, o);
          } else if (versioned && column.isVersion()) {
            // if version is null ignores it
            if (o != null) {
              String as = "_" + alias + "_";
              if (conditions != null) {
                conditions.add(column.eq(param(as)));
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
