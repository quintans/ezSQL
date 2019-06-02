package com.github.quintans.ezSQL.dml;

import com.github.quintans.ezSQL.common.api.Updatable;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.translator.Translator;
import com.github.quintans.ezSQL.exception.OrmException;
import com.github.quintans.ezSQL.mapper.InsertMapper;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class InsertDSL<T extends InsertDSL<T>> extends CoreDSL {
  protected boolean hasKeyValue;

  public InsertDSL(Translator translator, Table table) {
    super(translator, table);
  }

  @Override
  public String computeSql() {
    return translator.getSql(this);
  }

  public T set(Column<?> col, Function value) {
    return innerSet(col, value);
  }

  public <C> T set(Column<C> col, C value) {
    return innerSet(col, value);
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

  @SuppressWarnings("unchecked")
  protected T innerSet(Column<?> col, Object value) {
    super.coreSet(col, value);
    if (table.getSingleKeyColumn() != null && col.isKey()) {
      this.hasKeyValue = (value != null);
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

  public Map<Column<?>, Function> getValues() {
    return this.values;
  }

  /**
   * Loads sets all the columns of the table to matching bean property
   *
   * @param bean The bean to match
   * @return this
   */
  @SuppressWarnings("unchecked")
  public T set(Object bean) {
    mapObject(bean, false);

    if (bean instanceof Updatable) {
      ((Updatable) bean).clear();
    }

    return (T) this;
  }

  protected void mapObject(Object object, boolean versioned) {
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

    InsertMapper insertMapper = getTranslator().findInsertMapper(object.getClass());
    for (Column<?> column : table.getColumns()) {
      String alias = column.getAlias();
      if (changed == null || column.isKey() || column.isVersion() || changed.contains(alias)) {
        insertMapper.map(getTranslator(), column, object, versioned)
            .onSuccess(o -> {
              if (versioned && column.isVersion() && o == null) {
                throw new OrmException("Undefined version for %s.%s",
                    object.getClass().getSimpleName(), alias);
              }
              this.coreSet(column, o);
            });
      }
    }
  }

}
