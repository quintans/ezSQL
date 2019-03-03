package com.github.quintans.ezSQL;

import com.github.quintans.jdbc.exceptions.PersistenceException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Function;

public class TransactionManager<T extends AbstractDb> {
    @FunctionalInterface
    public interface DataProvider {
        Connection getConnection() throws SQLException;
    }

    @FunctionalInterface
    public interface Callback<T extends AbstractDb> {
        void call(T db) throws Exception;
    }

    @FunctionalInterface
    public interface CallbackF<T extends AbstractDb, R> {
        R call(T db) throws Exception;
    }

    private DataProvider dataProvider;
    private Function<Connection, T> dbSupplier;

    public TransactionManager(DataProvider dataProvider, Function<Connection, T> dbSupplier) {
        this.dataProvider = dataProvider;
        this.dbSupplier = dbSupplier;
    }

    public void transactionNoResult(final Callback<T> callback) {
        execute(false, db -> {
            callback.call(db);
            return null;
        });
    }

    public <R> R transaction(final CallbackF<T, R> callback) {
        return execute(false, callback);
    }

    public void readOnlyNoResult(final Callback<T> callback) {
        execute(true, db -> {
            callback.call(db);
            return null;
        });
    }

    public <R> R readOnly(final CallbackF<T, R> callback) {
        return execute(true, callback);
    }

    private <R> R execute(boolean readOnly, final CallbackF<T, R> callback) {
        Connection conn = fetchConnection(dataProvider);
        try {
            if (readOnly != conn.isReadOnly()) {
                conn.setReadOnly(readOnly);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to disable auto commit ", e);
        }

        R result;
        try {
            try {
                result = callback.call(dbSupplier.apply(conn));
            } catch (Exception e) {
                rollback(conn);
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new RuntimeException(e);
                }
            }

            if (!readOnly) {
                commit(conn);
            } else {
                rollback(conn);
            }
        } finally {
            close(conn);
        }

        return result;
    }

    private void commit(Connection conn) {
        try {
            conn.commit();
        } catch (SQLException e) {
            throw new PersistenceException("Unable to commit connection", e);
        }
    }

    private void rollback(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException e) {
            throw new PersistenceException("Unable to rollback connection", e);
        }
    }

    private Connection fetchConnection(DataProvider dataProvider) {
        Connection conn;
        try {
            conn = dataProvider.getConnection();
        } catch (SQLException e) {
            throw new PersistenceException("Unable to getConnection connection from " + dataProvider, e);
        }
        if (conn == null) {
            throw new IllegalStateException("DataProvider returned null from getConnection(): " + dataProvider);
        }
        try {
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to disable auto commit ", e);
        }
        return conn;
    }

    protected void close(Connection conn) {
        try {
            conn.close();
        } catch (SQLException e) {
            throw new PersistenceException("Unable to close connection", e);
        }
    }

}
