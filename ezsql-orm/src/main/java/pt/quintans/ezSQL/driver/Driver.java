package pt.quintans.ezSQL.driver;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import pt.quintans.ezSQL.common.type.MyDate;
import pt.quintans.ezSQL.common.type.MyDateTime;
import pt.quintans.ezSQL.common.type.MyTime;
import pt.quintans.ezSQL.db.Column;
import pt.quintans.ezSQL.db.NullSql;
import pt.quintans.ezSQL.db.Sequence;
import pt.quintans.ezSQL.db.Table;
import pt.quintans.ezSQL.dml.AutoKeyStrategy;
import pt.quintans.ezSQL.dml.Delete;
import pt.quintans.ezSQL.dml.Function;
import pt.quintans.ezSQL.dml.Insert;
import pt.quintans.ezSQL.dml.Query;
import pt.quintans.ezSQL.dml.Update;
import pt.quintans.ezSQL.sp.SqlProcedure;

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
    String columnAlias(Function function, int position);
    

    // == DB type to JAVA type

    Object toIdentity(ResultSet rs, int columnIndex) throws SQLException;

    Boolean toBoolean(ResultSet rs, int columnIndex) throws SQLException;

    String toString(ResultSet rs, int columnIndex) throws SQLException;

    Byte toTiny(ResultSet rs, int columnIndex) throws SQLException;
    
    Short toShort(ResultSet rs, int columnIndex) throws SQLException;
    
    Integer toInteger(ResultSet rs, int columnIndex) throws SQLException;

    Long toLong(ResultSet rs, int columnIndex) throws SQLException;

    Double toDecimal(ResultSet rs, int columnIndex) throws SQLException;

    MyTime toTime(ResultSet rs, int columnIndex) throws SQLException;
    MyDate toDate(ResultSet rs, int columnIndex) throws SQLException;
    MyDateTime toDateTime(ResultSet rs, int columnIndex) throws SQLException;
    java.util.Date toTimestamp(ResultSet rs, int columnIndex) throws SQLException;

    InputStream toText(ResultSet rs, int columnIndex) throws SQLException;

    InputStream toBin(ResultSet rs, int columnIndex) throws SQLException;

    BigDecimal toBigDecimal(ResultSet rs, int columnIndex) throws SQLException;

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
	 * @param type the column type
	 * @return
	 */
    Object fromNull(NullSql o);
    
    <T> T fromDb(ResultSet rs, int columnIndex, int sqlType, Class<T> type) throws SQLException;

}
