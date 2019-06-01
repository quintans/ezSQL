package com.github.quintans.jdbc.transformers;

import java.sql.SQLException;
import java.util.Collection;

public interface IResultTransformer<T> {
	void collect(ResultSetWrapper rsw) throws SQLException;
	Collection<T> collection();
}
