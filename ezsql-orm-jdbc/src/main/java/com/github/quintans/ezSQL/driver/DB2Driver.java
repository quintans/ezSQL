package com.github.quintans.ezSQL.driver;

import com.github.quintans.ezSQL.dml.AutoKeyStrategy;

public class DB2Driver extends GenericDriver {

  @Override
  public AutoKeyStrategy getAutoKeyStrategy() {
    return AutoKeyStrategy.AFTER;
  }

}
