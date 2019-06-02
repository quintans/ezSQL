package com.github.quintans.ezSQL.driver;

import com.github.quintans.ezSQL.common.type.MyDate;
import com.github.quintans.ezSQL.common.type.MyDateTime;
import com.github.quintans.ezSQL.common.type.MyTime;
import com.github.quintans.ezSQL.db.NullSql;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.jdbc.AbstractNullPreparedStatementCallback;
import com.github.quintans.ezSQL.jdbc.AbstractPreparedStatementCallback;
import com.github.quintans.ezSQL.toolkit.io.AutoCloseInputStream;
import com.github.quintans.ezSQL.toolkit.io.BinStore;
import com.github.quintans.ezSQL.toolkit.io.TextStore;
import com.github.quintans.ezSQL.transformers.Record;
import com.github.quintans.jdbc.PreparedStatementCallback;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public abstract class GenericDriver implements Driver {
  private Calendar calendar = Calendar.getInstance();

  public GenericDriver() {
    super();
  }

  @Override
  public void setTimeZoneId(String tzId) {
    this.calendar = Calendar.getInstance(TimeZone.getTimeZone(tzId));
  }

  @Override
  public Calendar getCalendar() {
    return this.calendar;
  }

  @Override
  public boolean isPmdKnownBroken() {
    return false;
  }

  @Override
  public Object toDb(Object parameter) {
    Object val = parameter;
    if (val instanceof NullSql) {
      val = this.fromNull((NullSql) val);
    } else if (val instanceof Boolean) {
      val = this.fromBoolean((Boolean) val);
    } else if (val instanceof Byte) {
      val = this.fromTiny((Byte) val);
    } else if (val instanceof Short) {
      val = this.fromShort((Short) val);
    } else if (val instanceof Integer) {
      val = this.fromInteger((Integer) val);
    } else if (val instanceof Long) {
      val = this.fromLong((Long) val);
    } else if (val instanceof Double) {
      val = this.fromDecimal((Double) val);
    } else if (val instanceof BigDecimal) {
      val = this.fromBigDecimal((BigDecimal) val);
    } else if (val instanceof String) {
      val = this.fromString((String) val);
    } else if (val instanceof MyTime) {
      val = this.fromTime((Date) val);
    } else if (val instanceof MyDate) {
      val = this.fromDate((Date) val);
    } else if (val instanceof MyDateTime) {
      val = this.fromDateTime((Date) val);
    } else if (val instanceof Date) {
      val = this.fromTimestamp((Date) val);
    } else if (val instanceof TextStore) {
      TextStore txt = (TextStore) val;
      try {
        val = this.fromText(txt.getInputStream(), (int) txt.getSize());
      } catch (IOException e) {
        throw new PersistenceException("Unable to get input stream from TextCache!", e);
      }
    } else if (val instanceof BinStore) {
      BinStore bin = (BinStore) val;
      try {
        val = this.fromBin(bin.getInputStream(), (int) bin.getSize());
      } catch (IOException e) {
        throw new PersistenceException("Unable to get input stream from ByteCache!", e);
      }
    } else if (val instanceof char[]) {
      String txt = new String((char[]) val);
      InputStream is;
      try {
        is = IOUtils.toInputStream(txt, TextStore.DEFAULT_CHARSET);
        val = this.fromText(is, txt.length());
      } catch (IOException e) {
        throw new PersistenceException("Unable to get input stream from String!", e);
      }
    } else if (val instanceof byte[]) {
      byte[] bin = (byte[]) val;
      val = this.fromBin(new ByteArrayInputStream(bin), bin.length);
    } else {
      val = this.fromUnknown(val);
    }

    return val;
  }

  /**
   * Override this to implement specific null settings, if any.
   * Ex: In PostgreSQL 9.3 to set a null value to a bytea we must use PreparedStatement.setBytes(parameterIndex, null)<br>
   * instead of PreparedStatement.setNull(parameterIndex, type);
   *
   * @param type the column type
   * @return db type
   */
  protected PreparedStatementCallback fromNull(final NullSql type) {
    return new AbstractNullPreparedStatementCallback(type) {
      @Override
      public void execute(PreparedStatement ps, int columnIndex) throws SQLException {
        ps.setNull(columnIndex, type.getType());
      }
    };
  }

  protected Object fromIdentity(Object o) {
    return (o == null ? fromNull(NullSql.BIGINT) : o);
  }

  protected Object fromBoolean(Boolean o) {
    return (o == null ? fromNull(NullSql.BOOLEAN) : o);
  }

  protected Object fromString(String o) {
    return (o == null ? fromNull(NullSql.VARCHAR) : o);
  }

  protected Object fromTiny(Byte o) {
    return (o == null ? fromNull(NullSql.TINY) : o);
  }

  protected Object fromShort(Short o) {
    return (o == null ? fromNull(NullSql.SMALL) : o);
  }

  protected Object fromInteger(Integer o) {
    return (o == null ? fromNull(NullSql.INTEGER) : o);
  }

  protected Object fromLong(Long o) {
    return (o == null ? fromNull(NullSql.BIGINT) : o);
  }

  protected Object fromDecimal(Double o) {
    return (o == null ? fromNull(NullSql.DECIMAL) : o);
  }

  protected Object fromBigDecimal(BigDecimal o) {
    return (o == null ? fromNull(NullSql.DECIMAL) : o);
  }

  protected Object fromTime(final Date o) {
    if (o == null)
      return fromNull(NullSql.TIME);
    else
      return new AbstractPreparedStatementCallback(o) {
        @Override
        public void execute(PreparedStatement ps, int columnIndex) throws SQLException {
          ps.setTime(columnIndex, new Time(o.getTime()));
        }
      };
  }

  protected Object fromDate(final Date o) {
    if (o == null)
      return fromNull(NullSql.DATE);
    else
      return new AbstractPreparedStatementCallback(o) {
        @Override
        public void execute(PreparedStatement ps, int columnIndex) throws SQLException {
          ps.setDate(columnIndex, new java.sql.Date(o.getTime()));
        }
      };
  }

  protected Object fromDateTime(final Date o) {
    if (o == null)
      return fromNull(NullSql.TIMESTAMP);
    else
      return new AbstractPreparedStatementCallback(o) {
        @Override
        public void execute(PreparedStatement ps, int columnIndex) throws SQLException {
          ps.setTimestamp(columnIndex, new Timestamp(o.getTime()));
        }
      };
  }

  protected Object fromTimestamp(final Date o) {
    if (o == null)
      return fromNull(NullSql.TIMESTAMP);
    else
      return new AbstractPreparedStatementCallback(o) {
        @Override
        public void execute(PreparedStatement ps, int columnIndex) throws SQLException {
          // ps.setTimestamp(columnIndex, new java.sql.Timestamp(o.getTime()), Calendar.getInstance(timeZone));
          ps.setTimestamp(columnIndex, new Timestamp(o.getTime()), getCalendar());
        }
      };
  }

  protected Object fromText(final InputStream is, final int length) {
    if (is == null) {
      return fromNull(NullSql.CLOB);
    } else {
      return new AbstractPreparedStatementCallback(is) {
        @Override
        public void execute(PreparedStatement ps, int columnIndex) throws SQLException {
          ps.setAsciiStream(columnIndex, new AutoCloseInputStream(is), (int) length);
        }
      };
    }
  }

  protected Object fromBin(final InputStream is, final int length) {
    if (is == null) {
      return fromNull(NullSql.BLOB);
    } else {
      return new AbstractPreparedStatementCallback(is) {
        @Override
        public void execute(PreparedStatement ps, int columnIndex) throws SQLException {
          ps.setBinaryStream(columnIndex, new AutoCloseInputStream(is), length);
        }
      };
    }
  }

  protected Object fromUnknown(Object o) {
    if (o instanceof Date)
      return fromDate((Date) o);
    if (o instanceof Boolean)
      return fromBoolean((Boolean) o);
    else
      return o;
  }

  @Override
  public void prepareConnection(Connection connection) {
  }

  @Override
  public Record newRecord(Query query, ResultSetWrapper rsw) {
    return new Record(query, rsw);
  }

}