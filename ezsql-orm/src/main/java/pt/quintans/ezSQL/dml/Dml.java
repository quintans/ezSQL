package pt.quintans.ezSQL.dml;

import java.util.List;

import pt.quintans.ezSQL.AbstractDb;
import pt.quintans.ezSQL.db.Table;

public abstract class Dml<T extends Dml> extends DmlCore<T> {
	public Dml(AbstractDb db, Table table) {
		super(db, table);
	}

	// ===

	// WHERE ===

	@Override
	@SuppressWarnings("unchecked")
	public T where(Condition... restrictions) {
		return (T) super.where(restrictions);
	}

	@Override
	@SuppressWarnings("unchecked")
	public T where(List<Condition> restrictions) {
		return (T) super.where(restrictions);
	}

	// ===
}
