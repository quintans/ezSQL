package com.github.quintans.jdbc;

import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.jdbc.exceptions.PersistenceIntegrityConstraintException;
import com.github.quintans.jdbc.sp.SqlParameter;
import com.github.quintans.jdbc.sp.SqlParameterType;
import com.github.quintans.jdbc.transformers.IResultTransformer;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;
import com.github.quintans.jdbc.transformers.SimpleAbstractResultTransformer;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Class that simplifies the execution o JDBC
 *
 * @author paulo.quintans
 */
public class SimpleJdbc {
  private JdbcSession jdbcSession;

  private PreparedStatement batchStmt = null;
  private String batchSql;
  private int batchPending;

  public SimpleJdbc(JdbcSession jdbcSession) {
    this.jdbcSession = jdbcSession;
  }

  public JdbcSession getJdbcSession() {
    return jdbcSession;
  }

  public void batch(String sql, Object[] params) {
    batch(sql, null, params);
  }

  public int[] flushUpdate() {
    int rows[] = null;

    if (batchStmt != null && batchPending > 0) {
      try {
        rows = batchStmt.executeBatch();
      } catch (SQLException e) {
        throw new PersistenceException(e);
      } finally {
        closeQuietly(null, batchStmt);
      }
    }

    return rows;
  }

  public void batch(String sql, String[] keyColumns, Object[] params) {
    Connection conn = null;
    try {
      if (!sql.equals(batchSql)) {
        closeBatch();
        this.batchSql = sql;

        conn = jdbcSession.getConnection();
        /*
         * poor performance. this will ask the db for the meta data
         */
        // boolean retriveGenKeys = keyColumns != null && conn.getMetaData().supportsGetGeneratedKeys();
        boolean retriveGenKeys = keyColumns != null;

        if (retriveGenKeys)
          batchStmt = conn.prepareStatement(sql, keyColumns);
        else
          batchStmt = conn.prepareStatement(sql);
      }

      fillStatement(batchStmt, params);
      batchStmt.addBatch();
      batchPending++;
    } catch (SQLException e) {
      rethrow(e, sql, params);
    } finally {
      closeBatch();
    }
  }

  public void flushInsert() {
    flushInsert(null);
  }

  public List<Map<String, Object>> flushInsert(String[] keyColumns) {
    List<Map<String, Object>> keyList = null;

    if (batchStmt != null && batchPending > 0) {
      try {
        batchStmt.executeBatch();

        if (keyColumns != null) {
          keyList = new ArrayList<>();
          ResultSet rs = batchStmt.getGeneratedKeys();
          if (rs.next()) {
            Map<String, Object> keys = new LinkedHashMap<String, Object>();
            for (int i = 0; i < keyColumns.length; i++) {
              keys.put(keyColumns[i], rs.getObject(i + 1));
            }
            keyList.add(keys);
          }
        }
      } catch (SQLException e) {
        throw new PersistenceException(e);
      } finally {
        closeQuietly(null, batchStmt);
      }
    }

    return keyList;
  }

  public void closeBatch() {
    if (batchStmt != null) {
      PreparedStatement stmt = batchStmt;
      batchStmt = null;
      batchSql = null;
      closeQuietly(null, stmt);
    }
  }

  public int getPending() {
    return batchPending;
  }


  /**
   * Execute an SQL SELECT query with replacement parameters.<br>
   * The caller is responsible for closing the connection.
   *
   * @param <T>    The type of object that the handler returns
   * @param sql    The query to execute.
   * @param rt     The handler that converts the results into an object.
   * @param params The replacement parameters.
   * @return List of objects.
   */
  public <T> List<T> query(String sql, IResultTransformer<T> rt, Object... params) {
    return queryRange(sql, rt, 0, 0, params);
  }

  public <T> T queryUnique(String sql, IResultTransformer<T> rt, Object... params) {
    List<T> result = queryRange(sql, rt, 0, 1, params);
    if (result.size() == 1)
      return result.get(0);
    else
      return null;
  }

  /**
   * Execute an SQL SELECT query with replacement parameters.<br>
   * The caller is responsible for closing the connection.
   *
   * @param <T>      The type of object that the handler returns
   * @param sql      The query to execute.
   * @param rt       The handler that converts the results into an object.
   * @param firstRow The position of the first row. Starts at 1. If zero (0), ignores the positioning.
   * @param maxRows  The number of rows to return. Starts at 1. If zero (0), returns all.
   * @param params   The replacement parameters.
   * @return The list of objects returned by the handler.
   */
  public <T> List<T> queryRange(String sql, IResultTransformer<T> rt, int firstRow, int maxRows, Object... params) {

    PreparedStatement stmt = null;
    ResultSet rs = null;
    Collection<T> result = null;

    Connection conn = null;
    try {
      conn = jdbcSession.getConnection();
      if (firstRow > 0) {
        stmt = conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        // Set the fetch size and search results for the result set
        if (maxRows > 0)
          stmt.setFetchSize(maxRows);
        stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
      } else
        stmt = conn.prepareStatement(sql);

      fillStatement(stmt, params);
      rs = stmt.executeQuery();

      ResultSetWrapper rsw = new ResultSetWrapper(rs);

      if (firstRow > 0)
        rs.absolute(firstRow);

      int rowNum = 0;

      //while (rs.next() && (maxRows == 0 || rowNum < maxRows)) {
      while (rs.next()) {
        // if the returned rows are higher than the requested one we have a problem and we want to know about it
        if (maxRows != 0 && rowNum == maxRows) {
          throw new PersistenceException("The query returned more than one result!");
        }

        rt.collect(rsw);
        rowNum++;
      }

    } catch (SQLException e) {
      rethrow(e, sql, params);
    } finally {
      result = rt.collection();
      closeQuietly(rs, stmt);
    }

    if (result instanceof List) {
      return (List<T>) result;
    } else if (result != null) {
      return new ArrayList<>(result);
    } else {
      return null;
    }
  }

  /**
   * Execute an SQL SELECT query with named parameters returning a collection of objects.
   *
   * @param <T>    list return type
   * @param sql    The query to execute.
   * @param rt     The handler that converts the results into an object.
   * @param params The named parameters.
   * @return The transformed result
   */
  public <T> List<T> queryForList(String sql, IResultTransformer<T> rt, Map<String, Object> params) {
    return queryForList(sql, rt, 0, 0, params);
  }

  /**
   * Execute an SQL SELECT query with named parameters returning a collection of objects.
   *
   * @param <T>      list return type
   * @param sql      The query to execute.
   * @param rt       The handler that converts the results into an object.
   * @param firstRow
   * @param maxRows
   * @param params   The named parameters.
   * @return The transformed result
   */
  public <T> List<T> queryForList(String sql, IResultTransformer<T> rt, int firstRow, int maxRows, Map<String, Object> params) {
    RawSql rawSql = RawSql.of(sql);
    return queryRange(rawSql.getJdbcSql(), rt, firstRow, maxRows, rawSql.buildValues(params));
  }

  /**
   * Execute an SQL SELECT query with named parameters.
   *
   * @param sql    The query to execute.
   * @param params The named parameters.
   * @return list with array of objects representing each result row
   */
  public List<Object[]> queryForList(final String sql, Map<String, Object> params) {
    IResultTransformer<Object[]> rt = new SimpleAbstractResultTransformer<Object[]>() {
      @Override
      public Object[] transform(ResultSetWrapper rsw) throws SQLException {
        int[] columnTypes = rsw.getColumnTypes();
        int cnt = columnTypes.length;

        ResultSet rs = rsw.getResultSet();

        Object[] objs = new Object[cnt];
        for (int i = 0; i < cnt; i++) {
          objs[i] = rs.getObject(i + 1);
        }
        return objs;
      }
    };

    RawSql rawSql = RawSql.of(sql);
    return query(rawSql.getJdbcSql(), rt, 0, rawSql.buildValues(params));
  }

  /**
   * Execute an SQL SELECT query with named parameters returning the first result.
   *
   * @param <T>    the result object type
   * @param sql    The query to execute.
   * @param rt     The handler that converts the results into an object.
   * @param params The named parameters.
   * @return The transformed result
   */
  public <T> T queryForObject(String sql, final IResultTransformer<T> rt, Map<String, Object> params) {
    RawSql rawSql = RawSql.of(sql);
    return queryUnique(rawSql.getJdbcSql(), rt, rawSql.buildValues(params));
  }

  /**
   * Execute an SQL SELECT query with named parameters returning the first result as a Long.
   *
   * @param sql    The query to execute.
   * @param params The named parameters.
   * @return The result as a Long
   */
  public Long queryForLong(String sql, Map<String, Object> params) {
    IResultTransformer<Long> rt = new SimpleAbstractResultTransformer<Long>() {
      @Override
      public Long transform(ResultSetWrapper rsw) throws SQLException {
        return rsw.getResultSet().getLong(1);
      }
    };

    return queryForObject(sql, rt, params);
  }

  /**
   * Execute an SQL INSERT, UPDATE, or DELETE query.
   *
   * @param sql    The SQL to execute.
   * @param params The query replacement parameters.
   * @return The number of rows affected.
   */
  public int update(String sql, Object... params) {

    PreparedStatement stmt = null;
    int rows = 0;

    Connection conn = null;
    try {
      conn = jdbcSession.getConnection();
      stmt = conn.prepareStatement(sql);
      fillStatement(stmt, params);
      rows = stmt.executeUpdate();
    } catch (SQLException e) {
      rethrow(e, sql, params);
    } finally {
      closeQuietly(null, stmt);
    }

    return rows;
  }

  /**
   * Execute an named parameter SQL INSERT, UPDATE, or DELETE query.
   *
   * @param sql    The SQL to execute.
   * @param params The query named parameters.
   * @return The number of rows affected.
   */
  public int update(String sql, Map<String, Object> params) {
    RawSql rawSql = RawSql.of(sql);
    return update(rawSql.getJdbcSql(), rawSql.buildValues(params));
  }

  /**
   * Executa INSERT devolvendo uma array com as chaves geradas.
   * Se o translator não suportar a obtenção de chaves geradas, devolve null.
   *
   * @param sql            instrução sql (INSERT) a executar
   * @param keyColumnTypes as colunas chaves a devolver
   * @param params         os dados do registo
   * @return as chaves
   */
  public Object[] insert(String sql, ColumnType[] keyColumnTypes, Object... params) {
    PreparedStatement stmt = null;
    Object[] keys = null;

    Connection conn = null;
    try {
      conn = jdbcSession.getConnection();
      /*
       * poor performance. this will ask the db for the meta data
       */
      // boolean retrieveGenKeys = keyColumns != null && conn.getMetaData().supportsGetGeneratedKeys();

      boolean retrieveGenKeys = keyColumnTypes != null;
      if (retrieveGenKeys) {
        String[] keyColumns = new String[keyColumnTypes.length];
        for (int i = 0; i < keyColumnTypes.length; i++) {
          keyColumns[i] = keyColumnTypes[i].getColumn();
        }
        stmt = conn.prepareStatement(sql, keyColumns);
      } else {
        stmt = conn.prepareStatement(sql);
      }

      fillStatement(stmt, params);
      stmt.executeUpdate();

      if (retrieveGenKeys) {
        keys = new Object[keyColumnTypes.length];
        ResultSet rs = stmt.getGeneratedKeys();
        if (rs.next()) {
          for (int i = 0; i < keyColumnTypes.length; i++) {
            keys[i] = rs.getObject(i + 1, keyColumnTypes[i].getType());
          }
        }
      }

    } catch (SQLException e) {
      rethrow(e, sql, params);
    } finally {
      closeQuietly(null, stmt);
    }

    return keys;
  }

  /**
   * Fill the <code>PreparedStatement</code> replacement parameters with
   * the given objects.
   *
   * @param stmt   PreparedStatement to fill
   * @param params Query replacement parameters; <code>null</code> is a valid
   *               value to pass in.
   * @throws SQLException if a database access error occurs
   */
  private void fillStatement(PreparedStatement stmt, Object... params) throws SQLException {
    if (params == null) {
      return;
    }

    ParameterMetaData pmd = null;

    for (int i = 0; i < params.length; i++) {
      Object param = params[i];
      int parameterIndex = i + 1;
      if (param != null) {
        if (param instanceof PreparedStatementCallback) {
          ((PreparedStatementCallback) param).execute(stmt, parameterIndex);
        } else
          stmt.setObject(parameterIndex, param);
      } else {
        // VARCHAR works with many drivers regardless
        // of the actual column type. Oddly, NULL and
        // OTHER don't work with Oracle's drivers.
        int sqlType = Types.VARCHAR;

        if (!jdbcSession.isPmdKnownBroken()) {
          try {
            if (pmd == null) { // can be returned by implementations that don't support the method
              pmd = stmt.getParameterMetaData();
              if(pmd == null) {
                jdbcSession.setPmdKnownBroken(true);
              }
            } else {
              if (pmd.getParameterCount() < params.length) {
                throw new SQLException("Too many parameters: expected "
                    + pmd.getParameterCount() + ", was given " + params.length);
              }
            }
            sqlType = pmd.getParameterType(parameterIndex);
          } catch (SQLException e) {
            jdbcSession.setPmdKnownBroken(true);
          }
        }
        stmt.setNull(parameterIndex, sqlType);
      }
    }
  }

  /**
   * Close a <code>Statement</code>, avoid closing if null.
   *
   * @param rs   ResultSet to close.
   * @param stmt Statement to close.
   */
  private void closeQuietly(ResultSet rs, Statement stmt) {
    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException e) {
      }
    }
    if (stmt != null) {
      try {
        stmt.close();
      } catch (SQLException e) {
      }
    }
  }

  /**
   * Execute a stored procedure call.<br>
   * The caller is responsible for closing the connection.
   *
   * @param sql        The JDBC string with call to the procedure
   * @param parameters The list with the parameter values
   * @return output The values mapped by parameter name
   */
  public Map<String, Object> call(String sql, List<SqlParameter> parameters) {

    CallableStatement stmt = null;
    Map<String, Object> outParameters = new HashMap<String, Object>();

    Connection conn = null;
    try {
      conn = jdbcSession.getConnection();
      stmt = conn.prepareCall(sql);

      int pos = 0;
      for (SqlParameter parameter : parameters) {
        pos++;
        if (parameter.isOut()) {
          stmt.registerOutParameter(pos, parameter.getJdbcType());
        }
      }

      // set INs
      pos = 0;
      for (SqlParameter parameter : parameters) {
        pos++;
        if (parameter.isDefined()) {
          if (parameter.getValue() != null) {
            stmt.setObject(pos, parameter.getValue(), parameter.getJdbcType());
          } else {
            stmt.setNull(pos, parameter.getJdbcType());
          }
        }
      }

      stmt.execute();

      // obtem primeiro os results set
      pos = 0;
      for (SqlParameter parameter : parameters) {
        if (SqlParameterType.RESULTSET.equals(parameter.getType())) {
          ResultSet rs = (ResultSet) stmt.getObject(parameter.getName());
          ResultSetWrapper rsw = new ResultSetWrapper(rs);
          IResultTransformer<Object> rt = parameter.getRowTransformer();
          Collection<Object> result;

          try {
            while (rs.next()) {
              rt.collect(rsw);
            }
          } finally {
            result = rt.collection();
          }

          rs.close();
          outParameters.put(parameter.getName(), result);
        }
      }

      pos = 0;
      for (SqlParameter parameter : parameters) {
        pos++;
        if (parameter.isOut() && !SqlParameterType.RESULTSET.equals(parameter.getType())) {
          Object o = stmt.getObject(pos);
          outParameters.put(parameter.getName(), o);
        }
      }

    } catch (SQLException e) {
      rethrow(e, sql, parameters.toArray());
    } finally {
      closeQuietly(null, stmt);
    }

    return outParameters;
  }

  /**
   * Throws a new exception with a more informative error message.
   *
   * @param cause  The original exception that will be chained to the new
   *               exception when it's rethrown.
   * @param sql    The query that was executing when the exception happened.
   * @param params The query replacement parameters; <code>null</code> is a
   *               valid value to pass in.
   */
  private void rethrow(SQLException cause, String sql, Object... params) {

    String causeMessage = cause.getMessage();
    if (causeMessage == null) {
      causeMessage = "";
    }
    StringBuffer msg = new StringBuffer(causeMessage);

    msg.append("\nQuery: ");
    msg.append(sql);
    msg.append("\nParameters: ");

    if (params == null) {
      msg.append("[]");
    } else {
      msg.append(Arrays.deepToString(params));
    }

    if (cause instanceof SQLIntegrityConstraintViolationException) {
      throw new PersistenceIntegrityConstraintException(msg.toString(), cause);
    } else {
      throw new PersistenceException(msg.toString(), cause);
    }
  }
}
