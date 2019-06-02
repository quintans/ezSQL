package com.github.quintans.ezSQL.transformers;

import com.github.quintans.ezSQL.common.api.Value;
import com.github.quintans.ezSQL.common.type.MyDate;
import com.github.quintans.ezSQL.common.type.MyDateTime;
import com.github.quintans.ezSQL.common.type.MyTime;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.mapper.Row;
import com.github.quintans.ezSQL.toolkit.io.BinStore;
import com.github.quintans.ezSQL.toolkit.io.TextStore;
import com.github.quintans.ezSQL.toolkit.utils.Misc;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;

public class Record implements Row {
  private Driver driver;
  private ResultSetWrapper rsw;
  private int offset;

  public Record(Query query, ResultSetWrapper rsw) {
    this.driver = query.getDriver();
    this.rsw = rsw;
    this.offset = query.getTranslator().paginationColumnOffset(query);
  }

  public ResultSetWrapper getResultSetWrapper() {
    return rsw;
  }

  protected Object toIdentity(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Object o = rs.getObject(columnIndex);
    return (rs.wasNull() ? null : o);
  }

  protected Boolean toBoolean(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Boolean o = rs.getBoolean(columnIndex);
    return (rs.wasNull() ? null : o);
  }

  protected String toString(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    String o = rs.getString(columnIndex);
    return (rs.wasNull() ? null : o);
  }

  protected Byte toTiny(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Byte o = rs.getByte(columnIndex);
    return (rs.wasNull() ? null : o);
  }

  protected Short toShort(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Short o = rs.getShort(columnIndex);
    return (rs.wasNull() ? null : o);
  }

  protected Integer toInteger(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Integer o = rs.getInt(columnIndex);
    return (rs.wasNull() ? null : o);
  }

  protected Long toLong(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Long o = rs.getLong(columnIndex);
    return (rs.wasNull() ? null : o);
  }

  protected Double toDecimal(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Double o = rs.getDouble(columnIndex);
    return (rs.wasNull() ? null : o);
  }

  protected BigDecimal toBigDecimal(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    BigDecimal o = rs.getBigDecimal(columnIndex);
    return (rs.wasNull() ? null : o);
  }

  protected MyTime toTime(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Date o = rs.getTime(columnIndex);
    return (rs.wasNull() ? null : new MyTime(o.getTime()));
  }

  protected MyDate toDate(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Date o = rs.getDate(columnIndex);
    return (rs.wasNull() ? null : new MyDate(o.getTime()));
  }

  protected MyDateTime toDateTime(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Date o = rs.getTimestamp(columnIndex);
    return (rs.wasNull() ? null : new MyDateTime(o.getTime()));
  }

  protected Date toTimestamp(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Date o = rs.getTimestamp(columnIndex, driver.getCalendar());
    return (rs.wasNull() ? null : o);
  }

  protected InputStream toText(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    InputStream in = rs.getAsciiStream(columnIndex);
    if (in == null || rs.wasNull()) {
      return null;
    }
    return in;
  }

  protected InputStream toBin(ResultSetWrapper rsw, int columnIndex) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    InputStream in = rs.getBinaryStream(columnIndex);
    if (in == null || rs.wasNull()) {
      return null;
    }
    return in;
  }

  protected Object toDefault(ResultSetWrapper rsw, int position) throws SQLException {
    ResultSet rs = rsw.getResultSet();
    Object o = rs.getObject(position);
    return rs.wasNull() ? null : o;
  }

  @SuppressWarnings("unchecked")
  protected <T> T fromDb(ResultSetWrapper rsw, int position, Class<T> klass) throws SQLException {
    if (klass == null) {
      return (T) toDefault(rsw, position);
    } else {

      Object val = null;
      if (klass.isAssignableFrom(Boolean.class) || klass.isAssignableFrom(boolean.class))
        val = toBoolean(rsw, position);
      else if (klass.isAssignableFrom(BigDecimal.class))
        val = toBigDecimal(rsw, position);
      else if (klass.isAssignableFrom(Byte.class) || klass.isAssignableFrom(byte.class))
        val = toTiny(rsw, position);
      else if (klass.isAssignableFrom(Short.class) || klass.isAssignableFrom(short.class))
        val = toShort(rsw, position);
      else if (klass.isAssignableFrom(Integer.class) || klass.isAssignableFrom(int.class))
        val = toInteger(rsw, position);
      else if (klass.isAssignableFrom(Long.class) || klass.isAssignableFrom(long.class))
        val = toLong(rsw, position);
      else if (klass.isAssignableFrom(Double.class) || klass.isAssignableFrom(double.class))
        val = toDecimal(rsw, position);
      else if (klass.isAssignableFrom(Date.class))
        val = toTimestamp(rsw, position);
      else if (klass.isAssignableFrom(MyTime.class))
        val = toTime(rsw, position);
      else if (klass.isAssignableFrom(MyDate.class))
        val = toDate(rsw, position);
      else if (klass.isAssignableFrom(MyDateTime.class))
        val = toDateTime(rsw, position);
      else if (TextStore.class.isAssignableFrom(klass)) {
        val = new TextStore();
        Misc.copy(toText(rsw, position), (BinStore) val);
      } else if (BinStore.class.isAssignableFrom(klass)) {
        val = new BinStore();
        Misc.copy(toBin(rsw, position), (BinStore) val);
      } else if (klass.isEnum()) {
        int sqlType = rsw.getSqlType(position);
        if (Types.VARCHAR == sqlType) {
          String value = toString(rsw, position);
          if (value != null) {
            if (Value.class.isAssignableFrom(klass)) {
              for (final Object element : klass.getEnumConstants()) {
                Value<?> e = (Value<?>) element;
                if (e.value().equals(value)) {
                  val = element;
                  break;
                }
              }
            } else {
              for (final Object element : klass.getEnumConstants()) {
                Enum<?> e = (Enum<?>) element;
                if (e.name().equals(value)) {
                  val = element;
                  break;
                }
              }
            }
          }
        } else {
          Integer value = toInteger(rsw, position);
          if (value != null) {
            int v = value;
            if (Value.class.isAssignableFrom(klass)) {
              for (final Object element : klass.getEnumConstants()) {
                Value<?> e = (Value<?>) element;
                if (((Number) e.value()).intValue() == v) {
                  val = element;
                  break;
                }
              }
            } else {
              for (final Object element : klass.getEnumConstants()) {
                Enum<?> e = (Enum<?>) element;
                if (e.ordinal() == v) {
                  val = element;
                  break;
                }
              }
            }
          }
        }
      } else if (klass.isAssignableFrom(byte[].class)) {
        // Types.BLOB == sqlType || Types.LONGVARBINARY == sqlType
        // since will be setting all bytes, ByteCahce is not necessary
        InputStream in = toBin(rsw, position);
        if (in != null) {
          try {
            val = IOUtils.toByteArray(in);
          } catch (IOException e) {
            throw new PersistenceException("Unable to convert stream into bytes!", e);
          } finally {
            IOUtils.closeQuietly(in);
          }
        }
      } else if (klass.isAssignableFrom(String.class)) {
        int sqlType = rsw.getSqlType(position);
        if (Types.CLOB == sqlType || Types.LONGNVARCHAR == sqlType) {
          // since will be setting all bytes, TextCache is not necessary
          InputStream in = toText(rsw, position);
          if (in != null) {
            try {
              val = IOUtils.toString(in, TextStore.DEFAULT_CHARSET);
            } catch (IOException e) {
              throw new PersistenceException("Unable to convert stream into bytes!", e);
            } finally {
              IOUtils.closeQuietly(in);
            }
          }
        } else
          val = toString(rsw, position);
      }

      return (T) val;
    }
  }

  public <T> T get(int columnIndex, Class<T> type) {
    try {
      return fromDb(rsw, columnIndex + offset, type);
    } catch (SQLException e) {
      throw new PersistenceException(e);
    }
  }

  public Object get(int columnIndex) {
    try {
      ResultSet rs = rsw.getResultSet();
      Object value = rs.getObject(columnIndex);
      return rs.wasNull() ? null : value;
    } catch (SQLException e) {
      throw new PersistenceException(e);
    }
  }
}
