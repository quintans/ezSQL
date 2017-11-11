package com.github.quintans.ezSQL.driver;


public interface UpdateBuilder {
    String getColumnPart();
    String getTablePart();
    String getWherePart();
}
