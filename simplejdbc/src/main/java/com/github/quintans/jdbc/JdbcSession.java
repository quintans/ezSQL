package com.github.quintans.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;

public interface JdbcSession {
    Connection getConnection();
    void returnConnection(Connection connection);
    boolean getPmdKnownBroken();
    void setPmdKnownBroken(boolean pmdKnownBroken);
}
