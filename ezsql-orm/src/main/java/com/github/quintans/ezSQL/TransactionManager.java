package com.github.quintans.ezSQL;

import com.github.quintans.jdbc.exceptions.PersistenceException;

import java.sql.Connection;
import java.sql.SQLException;

public class TransactionManager<T extends AbstractDb> {
    @FunctionalInterface
    public interface Callback<T extends AbstractDb> {
        void call(T db) throws Exception;
    }

    @FunctionalInterface
    public interface CallbackF<T extends AbstractDb, R> {
        R call(T db) throws Exception;
    }

    @FunctionalInterface
    public interface DbSupplier<T extends AbstractDb> {
        T get() throws SQLException;
    }

    private DbSupplier<T> dbSupplier;
    private boolean nested;

    public TransactionManager(DbSupplier<T> dbSupplier) {
        this(dbSupplier, false);
    }

    public TransactionManager(DbSupplier<T> dbSupplier, boolean nested) {
        this.dbSupplier = dbSupplier;
        this.nested = nested;
    }

    public static <T extends AbstractDb> TransactionManager nest(T db) {
        return new TransactionManager(() -> db, true);
    }

    public void transactionNoResult(final Callback<T> callback) {
        execute(db -> {
            callback.call(db);
            return null;
        });
    }

    public <R> R transaction(final CallbackF<T, R> callback) {
        return execute(callback);
    }

    private <R> R execute(final CallbackF<T, R> callback) {
        T db;
        try {
            db = dbSupplier.get();
        } catch (SQLException e) {
            throw new PersistenceException("Unable to get the database handler", e);
        }
        Connection conn = db.getConnection();
        if (conn == null) {
            throw new IllegalStateException("DataProvider returned null for the database connection");
        }

        if (!nested) {
            try {
                conn.setAutoCommit(false);
            } catch (SQLException e) {
                throw new IllegalStateException("Unable to disable auto commit ", e);
            }
        }

        R result = null;
        try {
            result = callback.call(db);
            if (!nested) {
                commit(conn);
            }
        } catch (Exception e) {
            if (!nested) {
                rollback(conn);
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            if (!nested) {
                close(conn);
            }
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

    protected void close(Connection conn) {
        try {
            conn.close();
        } catch (SQLException e) {
            throw new PersistenceException("Unable to close connection", e);
        }
    }

}
