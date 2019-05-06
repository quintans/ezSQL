package com.github.quintans.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface PreparedStatementCallback {
	void execute(PreparedStatement ps, int columnIndex) throws SQLException;
}
