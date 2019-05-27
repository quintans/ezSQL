package com.github.quintans.ezSQL.dml;

public class Union {
  private QueryDSL query;
  private boolean all;

  public Union(QueryDSL query, boolean all) {
    this.query = query;
    this.all = all;
  }

  @SuppressWarnings("unchecked")
  public <T extends QueryDSL> T getQuery() {
    return (T) query;
  }

  public boolean isAll() {
    return all;
  }
}
