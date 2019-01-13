package com.github.quintans.ezSQL.driver;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.NullSql;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.AutoKeyStrategy;
import com.github.quintans.ezSQL.dml.Delete;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.jdbc.PreparedStatementCallback;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

public class MySQLDriver extends GenericDriver {
    private final String DATE_FORMAT = "yyyy-MM-dd";
    private final String TIME_FORMAT = "HH:mm:ss";
    private final String DATETIME_FORMAT = DATE_FORMAT + "'T'" + TIME_FORMAT;
    private final String TIMESTAMP_FORMAT = DATETIME_FORMAT + ".SSS";
    private final String ISO_FORMAT = TIMESTAMP_FORMAT + " zzz";
    //private final String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS zzz";
    private String TIME_ZONE = "UTC";
        
    public MySQLDriver(){
        setTimeZoneId(TIME_ZONE);
    }
    
	@Override
	public String getAutoNumberQuery(Column<? extends Number> column, boolean current) {
		if (column.isKey())
			return "SELECT LAST_INSERT_ID()";
		else
			throw new PersistenceException(String.format("column '%s' must be key.", column));
	}

	@Override
	public boolean useSQLPagination() {
		return true;
	}

    @Override
    public String paginate(Query query, String sql) {
        StringBuilder sb = new StringBuilder();
        if (query.getLimit() > 0) {
	        sb.append(sql).append(" LIMIT :").append(Query.LAST_RESULT);
            query.setParameter(Query.LAST_RESULT, query.getLimit());
            if (query.getSkip() > 0) {
                sb.append(", :").append(Query.FIRST_RESULT);
                query.setParameter(Query.FIRST_RESULT, query.getSkip());
            }
	        return sb.toString();
	    }

	    return sql;
	}	

    @Override
    public AutoKeyStrategy getAutoKeyStrategy() {
        return AutoKeyStrategy.RETURNING;
    }
    
    @Override
    public java.util.Date toTimestamp(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    	ResultSet rs = rsw.getResultSet();
        // There is a bug in the mariadb driver. if the column value is null there is a null pointer exception if using a calendar
        Date o = rs.getTimestamp(columnIndex, getCalendar());
        //Date o = rs.getTimestamp(columnIndex);
        return (rs.wasNull() ? null : o);
    }
    
    @Override
	public String tableName(Table table) {
	    return "`" + table.getName().toUpperCase() + "`";
	}

    @Override
	public String columnName(Column<?> column) {
        return "`" + column.getName().toUpperCase() + "`";
	}

    @Override
    public Object fromDateTime(final java.util.Date o) {
        if (o == null)
            return NullSql.DATE;
        else
            return new PreparedStatementCallback() {
                @Override
                public void execute(PreparedStatement ps, int columnIndex) throws SQLException {
                    // the only way to force a time zone adjustment in MySQL
                    /*
                    SimpleDateFormat sdf = new SimpleDateFormat(DATETIME_FORMAT);
                    sdf.setTimeZone(getCalendar().getTimeZone());
                    ps.setString(columnIndex, sdf.format(o));
                    */
                    ps.setTimestamp(columnIndex, new java.sql.Timestamp(o.getTime()));
                }
            };
    }
    
    @Override
    protected DeleteBuilder createDeleteBuilder(Delete delete) {
        return new GenericDeleteBuilder(delete) {

            @Override
            public void from() {
                Table table = delete.getTable();
                String alias = delete.getTableAlias();
                tablePart.addAsOne(alias, " USING ", this.driver().tableName(table), " AS ", alias);
            }
            
        };
    }

}
