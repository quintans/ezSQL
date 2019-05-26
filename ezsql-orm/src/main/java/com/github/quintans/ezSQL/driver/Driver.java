package com.github.quintans.ezSQL.driver;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.github.quintans.ezSQL.common.type.MyDate;
import com.github.quintans.ezSQL.common.type.MyDateTime;
import com.github.quintans.ezSQL.common.type.MyTime;
import com.github.quintans.ezSQL.db.*;
import com.github.quintans.ezSQL.dml.AutoKeyStrategy;
import com.github.quintans.ezSQL.dml.Delete;
import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.dml.Insert;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.dml.Update;
import com.github.quintans.ezSQL.sp.SqlProcedure;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

public interface Driver {
    String translate(EDml dmlType, Function function);

    String getSql(SqlProcedure procedure);

    String getSql(Insert insert);

    String getSql(Query query);

    String getSql(Update update);

    String getSql(Delete delete);

    String getSql(Sequence sequence, boolean nextValue);

    AutoKeyStrategy getAutoKeyStrategy();

    String getAutoNumberQuery(Column<? extends Number> column);

    String getCurrentAutoNumberQuery(Column<? extends Number> column);

    boolean useSQLPagination();

    boolean ignoreNullKeys();

    int paginationColumnOffset(Query query);

    void prepareConnection(Connection connection);
    
    String tableName(Table table);
    String tableAlias(String alias);
    String columnName(Column<?> column);
    String procedureName(SqlProcedure procedure);
    String columnAlias(Function function, int position);
    
    boolean isPmdKnownBroken();

    // == DB type to JAVA type

    Object toIdentity(ResultSetWrapper rsw, int columnIndex) throws SQLException;

    Boolean toBoolean(ResultSetWrapper rsw, int columnIndex) throws SQLException;

    String toString(ResultSetWrapper rsw, int columnIndex) throws SQLException;

    Byte toTiny(ResultSetWrapper rsw, int columnIndex) throws SQLException;
    
    Short toShort(ResultSetWrapper rsw, int columnIndex) throws SQLException;
    
    Integer toInteger(ResultSetWrapper rsw, int columnIndex) throws SQLException;

    Long toLong(ResultSetWrapper rsw, int columnIndex) throws SQLException;

    Double toDecimal(ResultSetWrapper rsw, int columnIndex) throws SQLException;

    MyTime toTime(ResultSetWrapper rsw, int columnIndex) throws SQLException;
    MyDate toDate(ResultSetWrapper rsw, int columnIndex) throws SQLException;
    MyDateTime toDateTime(ResultSetWrapper rsw, int columnIndex) throws SQLException;
    java.util.Date toTimestamp(ResultSetWrapper rsw, int columnIndex) throws SQLException;

    InputStream toText(ResultSetWrapper rsw, int columnIndex) throws SQLException;

    InputStream toBin(ResultSetWrapper rsw, int columnIndex) throws SQLException;

    BigDecimal toBigDecimal(ResultSetWrapper rsw, int columnIndex) throws SQLException;

    // == JAVA type to DB type

    Object fromIdentity(Object o);

    Object fromBoolean(Boolean o);

    Object fromString(String o);

    Object fromTiny(Byte o);

    Object fromShort(Short o);
    
    Object fromInteger(Integer o);

    Object fromLong(Long o);

    Object fromDecimal(Double o);

    /**
     * Converts a java Date to a database Time
     *  
     * @param o
     * @return
     */
    Object fromTime(java.util.Date o);
    
    /**
     * Converts a java Date to a database Date
     * 
     * @param o
     * @return
     */
    Object fromDate(java.util.Date o);
    
    /**
     * Converts a java Date to a database Date and Time. Is the same as Timestamp but without timezone convertions.
     * 
     * @param o
     * @return
     */
    Object fromDateTime(java.util.Date o);
    
    Object fromTimestamp(java.util.Date o);

    Object fromText(InputStream is, int length);

    Object fromBin(InputStream is, int length);

    Object fromBigDecimal(BigDecimal bc);

    Object fromUnknown(Object o);

	/**
	 * Override this to implement specific null settings, if any.
	 * Ex: In PostgreSQL 9.3 to set a null value to a bytea we must use PreparedStatement.setBytes(parameterIndex, null)<br>
	 * instead of PreparedStatement.setNull(parameterIndex, type);
	 * 
	 * @param o the column type
	 * @return
	 */
    Object fromNull(NullSql o);
    
    <T> T fromDb(ResultSetWrapper rsw, int columnIndex, Class<T> type) throws SQLException;

}
