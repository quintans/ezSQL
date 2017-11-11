package com.github.quintans.ezSQL.sql;

import java.sql.Connection;
import java.sql.ResultSet;

public interface JdbcSession {
    Connection getConnection();
    void returnConnection(Connection connection);
    boolean getPmdKnownBroken();
    void setPmdKnownBroken(boolean pmdKnownBroken);
    int[] fetchColumnTypesForSelect(String sql, ResultSet resultSet);
}
