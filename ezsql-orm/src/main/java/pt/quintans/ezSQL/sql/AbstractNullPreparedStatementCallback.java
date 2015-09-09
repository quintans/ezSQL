package pt.quintans.ezSQL.sql;

import pt.quintans.ezSQL.db.NullSql;

public abstract class AbstractNullPreparedStatementCallback implements PreparedStatementCallback {
    private NullSql type;
    
    public AbstractNullPreparedStatementCallback(NullSql type) {
        this.type = type;
    }

	@Override
	public String toString() {
		return "null." + type.name();
	}

}
