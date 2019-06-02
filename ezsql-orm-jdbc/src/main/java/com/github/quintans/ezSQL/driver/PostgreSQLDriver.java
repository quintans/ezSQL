package com.github.quintans.ezSQL.driver;

import com.github.quintans.ezSQL.db.NullSql;
import com.github.quintans.ezSQL.dml.AutoKeyStrategy;
import com.github.quintans.ezSQL.jdbc.AbstractNullPreparedStatementCallback;
import com.github.quintans.jdbc.PreparedStatementCallback;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PostgreSQLDriver extends GenericDriver {
  private String TIME_ZONE = "UTC";

  private AbstractNullPreparedStatementCallback nullBlob = new AbstractNullPreparedStatementCallback(NullSql.BLOB) {
    @Override
    public void execute(PreparedStatement ps, int columnIndex) throws SQLException {
      ps.setBytes(columnIndex, null);
    }
  };

  private AbstractNullPreparedStatementCallback nullClob = new AbstractNullPreparedStatementCallback(NullSql.CLOB) {
    @Override
    public void execute(PreparedStatement ps, int columnIndex) throws SQLException {
      ps.setBytes(columnIndex, null);
    }
  };

  public PostgreSQLDriver() {
    setTimeZoneId(TIME_ZONE);
  }
  @Override
  public AutoKeyStrategy getAutoKeyStrategy() {
    return AutoKeyStrategy.RETURNING;
  }


  @Override
  public PreparedStatementCallback fromNull(NullSql type) {
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
