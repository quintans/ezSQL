package com.github.quintans.ezSQL.driver;

import com.github.quintans.ezSQL.db.NullSql;
import com.github.quintans.ezSQL.dml.AutoKeyStrategy;
import com.github.quintans.jdbc.PreparedStatementCallback;

import java.sql.Timestamp;
import java.util.Date;

public class MySQLDriver extends GenericDriver {
  private final String DATE_FORMAT = "yyyy-MM-dd";
  private final String TIME_FORMAT = "HH:mm:ss";
  private final String DATETIME_FORMAT = DATE_FORMAT + "'T'" + TIME_FORMAT;
  private final String TIMESTAMP_FORMAT = DATETIME_FORMAT + ".SSS";
  private final String ISO_FORMAT = TIMESTAMP_FORMAT + " zzz";
  //private final String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS zzz";
  private String TIME_ZONE = "UTC";

  public MySQLDriver() {
    super();
    setTimeZoneId(TIME_ZONE);
  }

  @Override
  public AutoKeyStrategy getAutoKeyStrategy() {
    return AutoKeyStrategy.RETURNING;
  }

  @Override
  public Object fromDateTime(final Date o) {
    if (o == null)
      return fromNull(NullSql.DATE);
    else
      return (PreparedStatementCallback) (ps, columnIndex) -> {
        // the only way to force a time zone adjustment in MySQL
                  /*
                  SimpleDateFormat sdf = new SimpleDateFormat(DATETIME_FORMAT);
                  sdf.setTimeZone(getCalendar().getTimeZone());
                  ps.setString(columnIndex, sdf.format(o));
                  */
        ps.setTimestamp(columnIndex, new Timestamp(o.getTime()));
      };
  }
}
