package com.github.quintans.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;

public interface JdbcSession {
    Connection getConnection();
    boolean getPmdKnownBroken();
    void setPmdKnownBroken(boolean pmdKnownBroken);
}
