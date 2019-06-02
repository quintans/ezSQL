package com.github.quintans.ezSQL.translator;


public interface InsertBuilder {
  String getColumnPart();

  String getValuePart();

  String getTablePart();
}
