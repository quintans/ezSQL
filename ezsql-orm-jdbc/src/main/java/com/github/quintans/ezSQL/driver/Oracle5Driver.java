package com.github.quintans.ezSQL.driver;


import com.github.quintans.ezSQL.dml.AutoKeyStrategy;

public class Oracle5Driver extends GenericDriver {
  public boolean useSQLPagination() {
    return false;
  }

  @Override
  public AutoKeyStrategy getAutoKeyStrategy() {
    return AutoKeyStrategy.BEFORE;
  }
}
