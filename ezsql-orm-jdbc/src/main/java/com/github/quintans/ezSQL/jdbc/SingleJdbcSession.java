package com.github.quintans.ezSQL.jdbc;

import java.sql.Connection;

public class SingleJdbcSession extends AbstractJdbcSession {
    private Connection connection;

    public SingleJdbcSession(Connection connection) {
        this(connection, false);
    }
    public SingleJdbcSession(Connection connection, boolean pmdKnownBroken) {
        super(pmdKnownBroken);
        this.connection = connection;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }
}
