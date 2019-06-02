package com.github.quintans.ezSQL.db;

import java.sql.Types;

public enum NullSql {
  UNKNOWN(Types.NULL),
  BOOLEAN(Types.BOOLEAN),
  CHAR(Types.CHAR),
  VARCHAR(Types.VARCHAR),
  BIGINT(Types.BIGINT),
  TINY(Types.TINYINT),
  SMALL(Types.SMALLINT),
  INTEGER(Types.INTEGER),
  DECIMAL(Types.DECIMAL),
  TIME(Types.TIME),
  DATE(Types.DATE),
  DATETIME(Types.TIMESTAMP), // local timezone
  TIMESTAMP(Types.TIMESTAMP),
  CLOB(Types.CLOB),
  BLOB(Types.BLOB);

  private int type;

  public int getType() {
    return type;
  }

  NullSql(int type) {
    this.type = type;
  }

  @Override
  public String toString() {
    return "NULL." + this.name();
  }

}
