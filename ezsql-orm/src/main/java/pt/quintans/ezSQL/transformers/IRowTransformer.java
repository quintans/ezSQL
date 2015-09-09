package pt.quintans.ezSQL.transformers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

public interface IRowTransformer<T> {
	boolean isFetchSqlTypes();

	/**
	 * Initializes the collection that will hold the results
	 * 
	 * @param rs
	 *            The ResultSet
	 * @return The collection
	 */
	Collection<T> beforeAll(ResultSet rs);

	T transform(ResultSet rs, int[] columnTypes) throws SQLException;

	/**
	 * Executes additional decision/action over the transformed object.<br>
	 * For example, It can decide not to include in the result if repeated...
	 * 
	 * @param object
	 *            The transformed object
	 */
	void onTransformation(Collection<T> result, T object);

	void afterAll(Collection<T> result);
}
