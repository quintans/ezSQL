package com.github.quintans.ezSQL.jdbc;

import java.io.InputStream;

import com.github.quintans.jdbc.PreparedStatementCallback;

public abstract class AbstractPreparedStatementCallback implements PreparedStatementCallback {
    private Object value;
    
    public AbstractPreparedStatementCallback(Object value) {
        this.value = value;
    }

    @Override
    public String toString(){
        if(value instanceof InputStream) {
            return "[byte]";
        } else {
            return String.valueOf(value);
        }
    }
}
