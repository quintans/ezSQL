package com.github.quintans.ezSQL.transformers;

import com.github.quintans.ezSQL.dml.Query;

public interface IQueryRowTransformer<T> extends IRowTransformer<T>{
	Query getQuery();
}
