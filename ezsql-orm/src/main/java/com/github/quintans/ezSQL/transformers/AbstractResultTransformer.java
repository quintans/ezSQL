package com.github.quintans.ezSQL.transformers;

import com.github.quintans.ezSQL.common.api.Updatable;
import com.github.quintans.jdbc.transformers.IResultTransformer;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

import java.sql.SQLException;
import java.util.Collection;

public abstract class AbstractResultTransformer<T> implements IResultTransformer<T> {

  abstract public T transform(ResultSetWrapper rsw) throws SQLException;

  @Override
  public void collect(ResultSetWrapper rsw) throws SQLException {
    T object = transform(rsw);
    if (object instanceof Updatable) {
      ((Updatable) object).clear();
    }
  }

  @Override
  public Collection<T> collection() {
    return null;
  }

}
