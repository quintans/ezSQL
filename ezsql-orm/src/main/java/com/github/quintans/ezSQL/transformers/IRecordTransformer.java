package com.github.quintans.ezSQL.transformers;

import java.sql.SQLException;

@FunctionalInterface
public interface IRecordTransformer<T> {
    T transform(Record record) throws SQLException;
}
