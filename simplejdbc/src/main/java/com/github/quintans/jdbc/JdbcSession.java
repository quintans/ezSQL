package com.github.quintans.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;

public interface JdbcSession {
    Connection getConnection();
    boolean isPmdKnownBroken();
    void setPmdKnownBroken(boolean pmdKnownBroken);
}
