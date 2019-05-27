package com.github.quintans.ezSQL.driver;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.NullSql;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.AutoKeyStrategy;
import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.dml.QueryDSL;
import com.github.quintans.ezSQL.dml.UpdateDSL;
import com.github.quintans.ezSQL.jdbc.AbstractNullPreparedStatementCallback;
import com.github.quintans.jdbc.exceptions.PersistenceException;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PostgreSQLDriver extends GenericDriver {
    private String TIME_ZONE = "UTC";
    
    private AbstractNullPreparedStatementCallback nullBlob = new AbstractNullPreparedStatementCallback(NullSql.BLOB){
		@Override
		public void execute(PreparedStatement ps, int columnIndex)	throws SQLException {
			ps.setBytes(columnIndex, null);
		}
    };
        
    private AbstractNullPreparedStatementCallback nullClob = new AbstractNullPreparedStatementCallback(NullSql.CLOB){
		@Override
		public void execute(PreparedStatement ps, int columnIndex)	throws SQLException {
			ps.setBytes(columnIndex, null);
		}
    };
        
    public PostgreSQLDriver(){
        setTimeZoneId(TIME_ZONE);
    }

    @Override
    public String getAutoNumberQuery(Column<? extends Number> column, boolean current) {
        if (column.isKey())
            return String.format("SELECT %s('%s_%s_seq');", current ? "currval" : "nextval", tableName(column.getTable()), columnName(column));
        else
            throw new PersistenceException(String.format("column '%s' must be key.", column));
    }

    @Override
	public boolean useSQLPagination() {
		return true;
	}

    @Override
    public String paginate(QueryDSL query, String sql) {
        StringBuilder sb = new StringBuilder();
        if (query.getLimit() > 0) {
	        sb.append(sql).append(" LIMIT :").append(QueryDSL.LAST_RESULT);
            query.setParameter(QueryDSL.LAST_RESULT, query.getLimit());
            if (query.getSkip() > 0) {
                sb.append(" OFFSET :").append(QueryDSL.FIRST_RESULT);
                query.setParameter(QueryDSL.FIRST_RESULT, query.getSkip());
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
    public boolean ignoreNullKeys() {
        return true;       
    }
    
    @Override
    public String tableName(Table table) {
        return table.getName().toLowerCase();
    }

    @Override
    public String columnName(Column<?> column) {
        return column.getName().toLowerCase();
    }
    
    @Override
    public UpdateBuilder createUpdateBuilder(UpdateDSL update) {
        return new GenericUpdateBuilder(update) {
            @Override
            public void column(Column<?> column, Function token){
                this.columnPart.addAsOne(
                        driver().columnName(column),
                    " = ", driver().translate(EDml.UPDATE, token));
            }
        };
    }

	@Override
	public Object fromNull(NullSql type) {
		switch (type) {
		case CLOB:
			return nullClob;

		case BLOB:
			return nullBlob;

		default:
			return super.fromNull(type);
		}
	}
    
}
