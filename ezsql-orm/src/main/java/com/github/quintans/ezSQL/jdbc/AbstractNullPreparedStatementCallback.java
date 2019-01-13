package com.github.quintans.ezSQL.jdbc;

import com.github.quintans.ezSQL.db.NullSql;
import com.github.quintans.jdbc.PreparedStatementCallback;

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
