package com.github.quintans.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface PreparedStatementCallback {
	public void execute(PreparedStatement ps, int columnIndex) throws SQLException;
}
