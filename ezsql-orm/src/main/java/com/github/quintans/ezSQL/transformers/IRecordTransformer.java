package com.github.quintans.ezSQL.transformers;

import com.github.quintans.ezSQL.transformers.Record;

import java.sql.SQLException;

@FunctionalInterface
public interface IRecordTransformer<T> {
    T transform(Record record) throws SQLException;
}
