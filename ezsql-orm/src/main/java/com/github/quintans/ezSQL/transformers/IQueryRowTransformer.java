package com.github.quintans.ezSQL.transformers;

import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.jdbc.transformers.IRowTransformer;

public interface IQueryRowTransformer<T> extends IRowTransformer<T>{
	Query getQuery();
	void setQuery(Query query);
}
