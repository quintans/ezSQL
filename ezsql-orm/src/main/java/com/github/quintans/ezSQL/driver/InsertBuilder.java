package com.github.quintans.ezSQL.driver;


public interface InsertBuilder {
    String getColumnPart();
    String getValuePart();
    String getTablePart();
}
