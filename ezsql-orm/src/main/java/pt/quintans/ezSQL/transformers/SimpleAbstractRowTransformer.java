package pt.quintans.ezSQL.transformers;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.LinkedList;

import pt.quintans.ezSQL.AbstractDb;
import pt.quintans.ezSQL.common.api.Updatable;

/**
 * User: quintans
 * Date: 13-jun-2006
 * Time: 22:13:52
 */
public abstract class SimpleAbstractRowTransformer<T> extends AbstractRowTransformer<T> {
    
	public SimpleAbstractRowTransformer() {
	}

	public SimpleAbstractRowTransformer(AbstractDb db) {
		super(db, false);
	}

	public SimpleAbstractRowTransformer(AbstractDb db, boolean fetchSqlTypes) {
		super(db, fetchSqlTypes);
	}

	@Override
	public Collection<T> beforeAll(final ResultSet resultSet) {
		return new LinkedList<T>();
	}

	@Override
	public void onTransformation(Collection<T> result, T object) {
	    if(object instanceof Updatable) {
	        ((Updatable) object).clear();
	    }
		result.add(object);
	}

	@Override
	public void afterAll(Collection<T> result) {
	}

}
