package com.github.quintans.jdbc.transformers;

import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;

/**
 * User: quintans
 *
 * @param <T>
 */
public abstract class SimpleAbstractResultTransformer<T> implements IResultTransformer<T> {
  private Collection<T> collection = new LinkedList<>();

  public abstract T transform(ResultSetWrapper rsw) throws SQLException;

  @Override
  public void collect(ResultSetWrapper rsw) throws SQLException {
    collection.add(transform(rsw));
  }

  @Override
  public Collection<T> collection() {
    return collection;
  }

}
