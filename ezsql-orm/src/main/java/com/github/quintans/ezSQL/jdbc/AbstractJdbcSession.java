package com.github.quintans.ezSQL.jdbc;

import java.sql.Connection;
import java.sql.ParameterMetaData;

import com.github.quintans.jdbc.JdbcSession;

public abstract class AbstractJdbcSession implements JdbcSession {
    /**
     * Is {@link ParameterMetaData#getParameterType(int)} broken (have we tried it yet)?
     */
    private boolean pmdKnownBroken = false;
    

    @Override
    public abstract Connection getConnection();

    @Override
    public boolean getPmdKnownBroken() {
        return pmdKnownBroken;
    }

    @Override
    public void setPmdKnownBroken(boolean pmdKnownBroken) {
        this.pmdKnownBroken = pmdKnownBroken;
    }

}
