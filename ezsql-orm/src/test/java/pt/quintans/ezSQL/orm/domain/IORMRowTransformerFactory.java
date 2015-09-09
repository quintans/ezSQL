package pt.quintans.ezSQL.orm.domain;

import pt.quintans.ezSQL.db.Association;
import pt.quintans.ezSQL.transformers.IRowTransformer;
import pt.quintans.ezSQL.transformers.IRowTransformerFactory;

public interface IORMRowTransformerFactory<T> extends IRowTransformerFactory<T> {

	/**
	 * Creates secondary mapper. It references the state of the passed mapper. The state is chained.<br>
	 * Only the method RowTransformer.transformed is invoked
	 * 
	 * @param foreignKey
	 *            the association for witch I want to load the entity
	 * @param other
	 *            the reference to the another mapper.
	 * @return the mapper
	 */
	IRowTransformer<T> createTransformer(Association foreignKey, IRowTransformer<?> other);
}
