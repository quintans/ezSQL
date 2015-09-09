package pt.quintans.ezSQL.transformers;

import pt.quintans.ezSQL.dml.Query;

public interface IQueryRowTransformer<T> extends IRowTransformer<T>{
	Query getQuery();
}
