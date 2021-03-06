package com.github.quintans.ezSQL.driver;

import com.github.quintans.ezSQL.common.api.Converter;
import com.github.quintans.ezSQL.common.type.MyDate;
import com.github.quintans.ezSQL.common.type.MyDateTime;
import com.github.quintans.ezSQL.common.type.MyTime;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.NullSql;
import com.github.quintans.ezSQL.db.Sequence;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.AutoKeyStrategy;
import com.github.quintans.ezSQL.dml.DeleteDSL;
import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.dml.InsertDSL;
import com.github.quintans.ezSQL.dml.QueryDSL;
import com.github.quintans.ezSQL.dml.UpdateDSL;
import com.github.quintans.ezSQL.sp.SqlProcedure;
import com.github.quintans.ezSQL.mapper.DeleteMapper;
import com.github.quintans.ezSQL.mapper.InsertMapper;
import com.github.quintans.ezSQL.mapper.QueryMapper;
import com.github.quintans.ezSQL.mapper.UpdateMapper;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;

public interface Driver {
    String translate(EDml dmlType, Function function);

    String getSql(SqlProcedure procedure);

    String getSql(InsertDSL insert);

    String getSql(QueryDSL query);

    String getSql(UpdateDSL update);

    String getSql(DeleteDSL delete);

    String getSql(Sequence sequence, boolean nextValue);

    void registerQueryMappers(QueryMapper... mappers);

    QueryMapper findQueryMapper(Class<?> klass);

    void registerInsertMappers(InsertMapper... mappers);

    InsertMapper findInsertMapper(Class<?> klass);

    void registerDeleteMappers(DeleteMapper... mappers);

    DeleteMapper findDeleteMapper(Class<?> klass);

    void registerUpdateMappers(UpdateMapper... mappers);

    UpdateMapper findUpdateMapper(Class<?> klass);

    Converter getConverter(Class<? extends Converter> converter);

    AutoKeyStrategy getAutoKeyStrategy();

    String getAutoNumberQuery(Column<? extends Number> column);

    String getCurrentAutoNumberQuery(Column<? extends Number> column);

    boolean useSQLPagination();

    boolean ignoreNullKeys();

    int paginationColumnOffset(QueryDSL query);

    void prepareConnection(Connection connection);
    
    String tableName(Table table);
    String tableAlias(String alias);
    String columnName(Column<?> column);
    String procedureName(SqlProcedure procedure);
    String columnAlias(Function function, int position);
    
    boolean isPmdKnownBroken();

    Object transformParameter(Object parameter);

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
     * @param o date
     * @return db time
     */
    Object fromTime(java.util.Date o);
    
    /**
     * Converts a java Date to a database Date
     * 
     * @param o date
     * @return db date
     */
    Object fromDate(java.util.Date o);
    
    /**
     * Converts a java Date to a database Date and Time. Is the same as Timestamp but without timezone convertions.
     * 
     * @param o date
     * @return db time and date
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
	 * @return db type
	 */
    Object fromNull(NullSql o);
    
    <T> T fromDb(ResultSetWrapper rsw, int columnIndex, Class<T> type) throws SQLException;

}
