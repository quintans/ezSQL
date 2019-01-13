package com.github.quintans.jdbc.transformers;

import java.sql.SQLException;
import java.util.Collection;

public interface IRowTransformer<T> {
	/**
	 * Initializes the collection that will hold the results
	 * 
	 * @param rsw
	 *            The ResultSetWrapper
	 * @return The collection
	 */
	Collection<T> beforeAll(ResultSetWrapper rsw);

	T transform(ResultSetWrapper rsw) throws SQLException;

	/**
	 * Executes additional decision/action over the transformed object.<br>
	 * For example, It can decide not to include in the result if repeated...
	 * 
	 * @param result
	 * @param object
	 *            The transformed object
	 */
	void onTransformation(Collection<T> result, T object);

	void afterAll(Collection<T> result);
}
