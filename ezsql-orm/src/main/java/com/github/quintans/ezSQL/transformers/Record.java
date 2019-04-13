package com.github.quintans.ezSQL.transformers;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.toolkit.io.BinStore;
import com.github.quintans.ezSQL.toolkit.io.TextStore;
import com.github.quintans.ezSQL.toolkit.utils.Misc;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

import java.math.BigDecimal;
import java.sql.SQLException;

public class Record {
    private ResultSetWrapper rsw;
    private Driver driver;
    private int offset;

    public Record(Query query, ResultSetWrapper rsw) {
        AbstractDb db = query.getDb();
        this.rsw = rsw;
        this.driver = db.getDriver();
        this.offset = query.paginationColumnOffset();
    }

    public Object getIdentity(int columnIndex) throws SQLException {
        return driver.toIdentity(rsw, columnIndex + offset);
    }

    public Boolean getBoolean(int columnIndex) throws SQLException {
        return driver.toBoolean(rsw, columnIndex + offset);
    }

    public String getString(int columnIndex) throws SQLException {
        return driver.toString(rsw, columnIndex + offset);
    }

    public Byte getTiny(int columnIndex) throws SQLException {
        return driver.toTiny(rsw, columnIndex + offset);
    }

    public Short getShort(int columnIndex) throws SQLException {
        return driver.toShort(rsw, columnIndex + offset);
    }

    public Integer getInteger(int columnIndex) throws SQLException {
        return driver.toInteger(rsw, columnIndex + offset);
    }

    public Long getLong(int columnIndex) throws SQLException {
        return driver.toLong(rsw, columnIndex + offset);
    }

    public Double getDecimal(int columnIndex) throws SQLException {
        return driver.toDecimal(rsw, columnIndex + offset);
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return driver.toBigDecimal(rsw, columnIndex + offset);
    }

    public java.util.Date getTime(int columnIndex) throws SQLException {
        return driver.toTime(rsw, columnIndex + offset);
    }

    public java.util.Date getDate(int columnIndex) throws SQLException {
        return driver.toDate(rsw, columnIndex + offset);
    }

    public java.util.Date getDateTime(int columnIndex) throws SQLException {
        return driver.toDateTime(rsw, columnIndex + offset);
    }

    public java.util.Date getTimestamp(int columnIndex) throws SQLException {
        return driver.toTimestamp(rsw, columnIndex + offset);
    }

    public TextStore getText(int columnIndex) throws SQLException {
        TextStore val = new TextStore();
        Misc.copy(driver.toText(rsw, columnIndex + offset), val);
        return val;
    }

    public BinStore getBin(int columnIndex) throws SQLException {
        BinStore val = new BinStore();
        Misc.copy(driver.toBin(rsw, columnIndex + offset), val);
        return val;
    }

    public <T> T get(int columnIndex, Class<T> type) throws SQLException {
        return driver.fromDb(rsw, columnIndex + offset, type);
    }
}
