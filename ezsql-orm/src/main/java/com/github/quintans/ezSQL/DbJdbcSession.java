package com.github.quintans.ezSQL;

import com.github.quintans.ezSQL.jdbc.AbstractJdbcSession;

import java.sql.Connection;

public class DbJdbcSession extends AbstractJdbcSession {
    private AbstractDb db;

    public DbJdbcSession(AbstractDb db) {
        this.db = db;
    }

    @Override
    public Connection getConnection() {
        return db.getConnection();
    }

}
