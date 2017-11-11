package com.github.quintans.ezSQL.sql;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface PreparedStatementCallback {
	public void execute(PreparedStatement ps, int columnIndex) throws SQLException;
}
