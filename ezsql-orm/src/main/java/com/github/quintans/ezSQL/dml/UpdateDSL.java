package com.github.quintans.ezSQL.dml;

import com.github.quintans.ezSQL.common.api.Updatable;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.translator.Translator;
import com.github.quintans.ezSQL.exception.OrmException;
import com.github.quintans.ezSQL.mapper.UpdateMapper;
import com.github.quintans.ezSQL.mapper.UpdateValue;
import com.github.quintans.ezSQL.toolkit.utils.Result;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static com.github.quintans.ezSQL.dml.Definition.param;

public class UpdateDSL<T extends UpdateDSL<T>> extends CoreDSL {

  public UpdateDSL(Translator translator, Table table) {
    super(translator, table);
  }

  @Override
  public String computeSql() {
    return translator.getSql(this);
  }


  public Map<Column<?>, Function> getValues() {
    return this.values;
  }

  @SuppressWarnings("unchecked")
  public T set(Column<?> column) {
    coreSet(column, column.param());
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T set(Column<?> col, Function value) {
    coreSet(col, value);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public <C> T set(Column<C> col, C value) {
    coreSet(col, value);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public <C> T set(Column<C> col, Column<C> value) {
    coreSet(col, value);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public <C> C get(Column<C> col) {
    return (C) coreGet(col);
  }

  @SuppressWarnings("unchecked")
  public <C> T with(Column<C> c, C value) {
    setParameter(c, value);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T with(String name, Object value) {
    setParameter(name, value);
    return (T) this;
  }

  /**
   * Sets all the columns of the table to matching bean property.<br>
   * Version column is ignored.
   *
   * @param bean The bean to match
   * @return this
   */
  @SuppressWarnings("unchecked")
  public T set(Object bean) {
    mapBean(bean, false);

    if (bean instanceof Updatable) {
      ((Updatable) bean).clear();
    }

    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T sets(Column<?>... columns) {
    coreSets(columns);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T values(Object... values) {
    coreValues(values);
    return (T) this;
  }

  protected IdVer mapBean(Object bean, boolean versioned) {
    this.parameters = new LinkedHashMap<>();
    this.values = new LinkedHashMap<>();

    List<Condition> conditions = null;
    if (bean.getClass() != this.lastBeanClass) {
      conditions = new ArrayList<Condition>();
      this.condition = null;
      this.lastBeanClass = bean.getClass();
      this.lastSql = null;
    }

    IdVer idVer = new IdVer();

    Set<String> changed = null;
    if (bean instanceof Updatable) {
      changed = ((Updatable) bean).changed();
    }

    for (Column<?> column : table.getColumns()) {
      String alias = column.getAlias();
      if (changed == null || column.isKey() || column.isVersion() || changed.contains(alias)) {
        UpdateMapper mapper = getTranslator().findUpdateMapper(bean.getClass());
        Result<UpdateValue> result = mapper.map(translator, column, bean);
        if (result.isSuccess()) {
          UpdateValue updateValue = result.get();
          Object o = updateValue.getCurrent();
          if (column.isKey()) {
            if (o == null)
              throw new OrmException("Value for key property '%s' cannot be null.", alias);

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

              this.coreSet(column, idVer.versionValue);
            }
          } else {
            this.coreSet(column, o);
          }
        }
      }
    }

    if (conditions != null) {
      where(conditions);
    }

    return idVer;
  }

  protected static class IdVer {
    boolean noId = true;
    Consumer<Object> setter = null;
    Object versionValue = null;
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
