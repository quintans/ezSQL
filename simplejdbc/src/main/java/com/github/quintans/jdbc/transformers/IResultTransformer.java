package com.github.quintans.jdbc.transformers;

import java.sql.SQLException;
import java.util.Collection;

public interface IResultTransformer<T> {
	/**
	 * Initializes the collection that will hold the results
	 * @return The collection
	 */
	Collection<T> beforeAll();

	T transform(ResultSetWrapper rsw) throws SQLException;

	/**
	 * Executes additional decision/action over the transformed object.<br>
	 * For example, It can decide not to include in the result if repeated...
	 * 
	 * @param result
	 * @param object
	 *            The transformed object
	 */
	void collect(Collection<T> result, T object);

	void afterAll(Collection<T> result);
}
