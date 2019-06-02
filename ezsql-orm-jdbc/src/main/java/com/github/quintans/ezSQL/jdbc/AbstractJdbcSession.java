package com.github.quintans.ezSQL.jdbc;

import java.sql.Connection;
import java.sql.ParameterMetaData;

import com.github.quintans.jdbc.JdbcSession;

public abstract class AbstractJdbcSession implements JdbcSession {
    /**
     * Is {@link ParameterMetaData#getParameterType(int)} broken (have we tried it yet)?
     */
    private boolean pmdKnownBroken = false;

    public AbstractJdbcSession(boolean pmdKnownBroken) {
        this.pmdKnownBroken = pmdKnownBroken;
    }

    @Override
    public abstract Connection getConnection();

    @Override
    public boolean isPmdKnownBroken() {
        return pmdKnownBroken;
    }

    @Override
    public void setPmdKnownBroken(boolean pmdKnownBroken) {
        this.pmdKnownBroken = pmdKnownBroken;
    }
}
