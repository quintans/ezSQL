package com.github.quintans.ezSQL.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import com.github.quintans.jdbc.PreparedStatementCallback;

public enum NullSql implements PreparedStatementCallback {
    UNKNOWN(Types.NULL),
    BOOLEAN(Types.BOOLEAN),
	CHAR(Types.CHAR), 
	VARCHAR(Types.VARCHAR), 
    BIGINT(Types.BIGINT), 
    TINY(Types.TINYINT), 
    SMALL(Types.SMALLINT), 
    INTEGER(Types.INTEGER), 
    DECIMAL(Types.DECIMAL), 
	TIME(Types.TIME),
    DATE(Types.DATE),
    DATETIME(Types.TIMESTAMP), // local timezone
    TIMESTAMP(Types.TIMESTAMP),
	CLOB(Types.CLOB), 
	BLOB(Types.BLOB); 
	
	private int type;

	public int getType() {
		return type;
	}

	private NullSql(int type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "NULL." + this.name();
	}

	@Override
	public void execute(PreparedStatement ps, int columnIndex) throws SQLException {
		ps.setNull(columnIndex, getType());
	}
}
