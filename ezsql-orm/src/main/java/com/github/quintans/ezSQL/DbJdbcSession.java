package com.github.quintans.ezSQL;

import com.github.quintans.ezSQL.jdbc.AbstractJdbcSession;

import java.sql.Connection;
import java.sql.Driver;

public class DbJdbcSession extends AbstractJdbcSession {
    private AbstractDb db;

    public DbJdbcSession(AbstractDb db) {
        this(db, false);
    }

    public DbJdbcSession(AbstractDb db, boolean pmdKnownBroken) {
        super(pmdKnownBroken);
        this.db = db;
    }

    @Override
    public Connection getConnection() {
        return db.getConnection();
    }

}
