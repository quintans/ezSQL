package com.github.quintans.ezSQL.driver;

import com.github.quintans.ezSQL.db.NullSql;
import com.github.quintans.ezSQL.dml.AutoKeyStrategy;
import com.github.quintans.jdbc.PreparedStatementCallback;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

public class OracleDriver extends GenericDriver {
  /**
   * maximum length for object names in Oracle
   */
  private static final int NAME_MAX_LEN = 30;

  @Override
  public AutoKeyStrategy getAutoKeyStrategy() {
    return AutoKeyStrategy.BEFORE;
  }

  @Override
  public Object fromBoolean(Boolean o) {
    if (o != null)
      return o ? "1" : "0";
    else
      return fromNull(NullSql.CHAR);
  }

  @Override
  public Object fromTimestamp(final Date o) {
    if (o == null)
      return fromNull(NullSql.DATE);
    else
      return new PreparedStatementCallback() {
        @Override
        public void execute(PreparedStatement ps, int columnIndex) throws SQLException {
          ps.setTimestamp(columnIndex, new Timestamp(o.getTime()), getCalendar());
        }
      };
  }

}
