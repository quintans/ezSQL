package com.github.quintans.ezSQL;

import java.sql.Connection;

import com.github.quintans.ezSQL.sql.AbstractJdbcSession;

public class DbJdbcSession extends AbstractJdbcSession {
    private AbstractDb db;

    public DbJdbcSession(AbstractDb db) {
        this.db = db;
    }

    @Override
    public Connection getConnection() {
        return db.getConnection();
    }

    @Override
    public void returnConnection(Connection connection) {
    }

}
