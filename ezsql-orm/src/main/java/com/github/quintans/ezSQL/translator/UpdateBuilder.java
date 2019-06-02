package com.github.quintans.ezSQL.translator;


public interface UpdateBuilder {
    String getColumnPart();
    String getTablePart();
    String getWherePart();
}
