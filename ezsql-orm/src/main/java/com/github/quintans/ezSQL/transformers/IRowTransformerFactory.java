package com.github.quintans.ezSQL.transformers;

import com.github.quintans.ezSQL.dml.Query;

public interface IRowTransformerFactory<T> {
	/**
	 * Creates the fist mapper. The first mapper is responsible for the resultset and for mapping to the main table
	 * 
	 * @param query
	 *            the query for this mapper. It gives information on how to map the columns to the result set position and hence ORM mapping
	 * @param reuse
	 *            if the result is a object tree or if it is flat
	 * @return the mapper
	 */
	IRowTransformer<T> createTransformer(Query query, boolean reuse);
}
