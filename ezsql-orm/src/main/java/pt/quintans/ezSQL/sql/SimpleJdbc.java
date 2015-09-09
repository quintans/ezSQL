package pt.quintans.ezSQL.sql;

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

import pt.quintans.ezSQL.exceptions.PersistenceException;
import pt.quintans.ezSQL.exceptions.PersistenceIntegrityConstraintException;
import pt.quintans.ezSQL.sp.SqlParameter;
import pt.quintans.ezSQL.sp.SqlParameterType;
import pt.quintans.ezSQL.transformers.IRowTransformer;
import pt.quintans.ezSQL.transformers.SimpleAbstractRowTransformer;

/**
 * Class that simplifies the execution o JDBC
 * 
 * @author paulo.quintans
 * 
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

        if(batchPending > 0) {
            try {
                rows = batchStmt.executeBatch();
            } catch (SQLException e) {
                Connection conn = null;
                try {
                    conn = batchStmt.getConnection();
                    close(batchStmt);
                } catch (Exception ex) {
                } finally {
                    close(conn);                
                }
                throw new PersistenceException(e);
            }
        }

        return rows;
    }

    public void batch(String sql, String[] keyColumns, Object[] params) {
        Connection conn = null;
        try {
            if(!sql.equals(batchSql)) {
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
            try {
                closeBatch();
            } catch (Exception ex) {
            }
            rethrow(e, sql, params);
        }
    }

    public void flushInsert() {
        flushInsert(null);
    }
    
    public List<Map<String, Object>> flushInsert(String[] keyColumns) {
        List<Map<String, Object>> keyList = null;

        if(batchPending > 0) {
            try {
                batchStmt.executeBatch();
    
                if (keyColumns != null) {
                    keyList = new ArrayList<Map<String, Object>>();
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
                Connection conn = null;
                try {
                    conn = batchStmt.getConnection();
                    close(batchStmt);
                } catch (SQLException ex) {
                } finally {
                    close(conn);                
                }
                throw new PersistenceException(e);
            }
        }

        return keyList;
    }
    
    public void closeBatch() {
        if(batchStmt != null) {
            PreparedStatement stmt = batchStmt;
            batchStmt = null;
            batchSql = null;
            Connection conn = null;
            try {
                conn = stmt.getConnection();
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException ex) {
                throw new PersistenceException(ex);
            } finally {
                close(conn);                
            }
        }
    }
    
    public int getPending() {
        return batchPending;
    }
    

	/**
	 * Execute an SQL SELECT query with replacement parameters.<br>
	 * The caller is responsible for closing the connection.
	 * 
	 * @param <T>
	 *            The type of object that the handler returns
	 * @param conn
	 *            The connection to execute the query in.
	 * @param sql
	 *            The query to execute.
	 * @param rt
	 *            The handler that converts the results into an object.
	 * @param params
	 *            The replacement parameters.
	 * @return The object returned by the handler.
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public <T> List<T> query(String sql, IRowTransformer<T> rt, Object... params) {
		return queryRange(sql, rt, 0, 0, params);
	}

    public <T> T queryUnique(String sql, IRowTransformer<T> rt, Object... params) {
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
	 * @param <T>
	 *            The type of object that the handler returns
	 * @param conn
	 *            The connection to execute the query in.
	 * @param sql
	 *            The query to execute.
	 * @param rst
	 *            The ResultSetTransformer. If <code>null</code>, it will use the default transformation.
	 * @param rt
	 *            The handler that converts the results into an object.
	 * @param firstRow
	 *            The position of the first row. Starts at 1. If zero (0), ignores the positioning.
	 * @param maxRows
	 *            The number of rows to return. Starts at 1. If zero (0), returns all.
	 * @param params
	 *            The replacement parameters.
	 * @return The object returned by the handler.
	 * @throws PersistenceException
	 *             if a database access error occurs
	 */
	public <T> List<T> queryRange(String sql, IRowTransformer<T> rt, int firstRow, int maxRows, Object... params) {

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

			if (firstRow > 0)
				rs.absolute(firstRow);

			// the returned collection can be null
			result = rt.beforeAll(rs);
			int rowNum = 0;

			int[] columnTypes = null;
			if (rt.isFetchSqlTypes())
				columnTypes = jdbcSession.fetchColumnTypesForSelect(sql, rs);

			//while (rs.next() && (maxRows == 0 || rowNum < maxRows)) {
			while (rs.next()) {
			    // if the returned rows are higher than the requested one we have a problem and we want to know about it
		        if (maxRows != 0 && rowNum == maxRows) {
		            throw new PersistenceException("The query returned more than one result!");
		        }
		        
				rt.onTransformation(result, rt.transform(rs, columnTypes));
				rowNum++;
			}

		} catch (SQLException e) {
			rethrow(e, sql, params);

		} finally {
			try {
	            rt.afterAll(result);
				close(rs);
			} catch (SQLException e) {
				// throw new PersistenceException(e);
			} finally {
				try {
					close(stmt);
				} catch (SQLException e) {
					// throw new PersistenceException(e);
	            } finally {
	                close(conn);                
				}
			}
		}

		if(result instanceof List) {
		    return (List<T>) result;
		} else if (result != null) {
		    return new ArrayList<T>(result);
		} else {
			return null;
		}
	}

	/**
	 * Execute an SQL SELECT query with named parameters returning a collection of objects.
	 * 
	 * @param <T>
	 *            list return type
	 * @param conn
	 *            The connection to execute the query in.
	 * @param sql
	 *            The query to execute.
	 * @param rt
	 *            The handler that converts the results into an object.
	 * @param params
	 *            The named parameters.
	 * @return The transformed result
	 */
	public <T> List<T> queryForList(String sql, IRowTransformer<T> rt, Map<String, Object> params) {
		return queryForList(sql, rt, 0, 0, params);
	}

	/**
	 * Execute an SQL SELECT query with named parameters returning a collection of objects.
	 * 
	 * @param <T>
	 *            list return type
	 * @param conn
	 *            The connection to execute the query in.
	 * @param sql
	 *            The query to execute.
	 * @param rt
	 *            The handler that converts the results into an object.
	 * @param params
	 *            The named parameters.
	 * @return The transformed result
	 */
	public <T> List<T> queryForList(String sql, IRowTransformer<T> rt, int firstRow, int maxRows, Map<String, Object> params) {
		RawSql rawSql = toRawSql(sql);
		return queryRange(rawSql.getSql(), rt, firstRow, maxRows, rawSql.buildValues(params));
	}

	/**
	 * converts SQL with named parameters to JDBC standard sql
	 * 
	 * @param sql
	 *            The SQL to be converted
	 * @param params
	 *            The named parameters and it's values
	 * @return The {@link RawSql} with the result
	 */
	public RawSql toRawSql(String sql) {
		ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement(sql);
        RawSql rawSql = new RawSql(parsedSql);
		return rawSql;
	}

	/**
	 * Execute an SQL SELECT query with named parameters.
	 * 
	 * @param conn
	 *            The connection to execute the query in.
	 * @param sql
	 *            The query to execute.
	 * @param params
	 *            The named parameters.
	 * @return list with array of objects representing each result row
	 */
	public List<Object[]> queryForList(final String sql, Map<String, Object> params) {
		IRowTransformer<Object[]> rt = new SimpleAbstractRowTransformer<Object[]>(null, true) {
			@Override
			public Object[] transform(ResultSet rs, int[] columnTypes) throws SQLException {
				int cnt = columnTypes.length;

				Object objs[] = new Object[cnt];
				for (int i = 0; i < cnt; i++) {
					objs[i] = rs.getObject(i + 1);
				}
				return objs;
			}
		};

		RawSql rawSql = toRawSql(sql);
		return query(rawSql.getSql(), rt, 0, rawSql.buildValues(params));
	}

	/**
	 * Execute an SQL SELECT query with named parameters returning the first result.
	 * 
	 * @param <T>
	 *            the result object type
	 * @param conn
	 *            The connection to execute the query in.
	 * @param sql
	 *            The query to execute.
	 * @param rt
	 *            The handler that converts the results into an object.
	 * @param params
	 *            The named parameters.
	 * @return The transformed result
	 */
	public <T> T queryForObject(String sql, final IRowTransformer<T> rt, Map<String, Object> params) {
		RawSql rawSql = toRawSql(sql);
		return queryUnique(rawSql.getSql(), rt, rawSql.buildValues(params));
	}

	/**
	 * Execute an SQL SELECT query with named parameters returning the first result as a Long.
	 * 
	 * @param conn
	 *            The connection to execute the query in.
	 * @param sql
	 *            The query to execute.
	 * @param params
	 *            The named parameters.
	 * @return The result as a Long
	 */
	public Long queryForLong(String sql, Map<String, Object> params) {
		IRowTransformer<Long> rt = new SimpleAbstractRowTransformer<Long>() {
			@Override
			public Long transform(ResultSet rs, int[] columnTypes) throws SQLException {
				return rs.getLong(1);
			}
		};

		return queryForObject(sql, rt, params);
	}

	/**
	 * Execute an SQL INSERT, UPDATE, or DELETE query.
	 * 
	 * @param conn
	 *            The connection to use to run the query.
	 * @param sql
	 *            The SQL to execute.
	 * @param params
	 *            The query replacement parameters.
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
			try {
				close(stmt);
			} catch (SQLException e) {
				throw new PersistenceException(e);
            } finally {
                close(conn);                
			}
		}

		return rows;
	}

	/**
	 * Execute an named parameter SQL INSERT, UPDATE, or DELETE query.
	 * 
	 * @param conn
	 *            The connection to use to run the query.
	 * @param sql
	 *            The SQL to execute.
	 * @param params
	 *            The query named parameters.
	 * @return The number of rows affected.
	 */
	public int update(String sql, Map<String, Object> params) {
		RawSql rawSql = toRawSql(sql);
		return update(rawSql.getSql(), rawSql.buildValues(params));
	}

	/**
	 * Executa INSERT devolvendo uma array com as chaves geradas.
	 * Se o driver não suportar a obtenção de chaves geradas, devolve null.
	 * 
	 * @param conn
	 *            a ligação
	 * @param sql
	 *            instrução sql (INSERT) a executar
	 * @param keyColumns
	 *            as colunas chaves a devolver
	 * @param params
	 *            os dados do registo
	 * @return as chaves
	 */
	public Object[] insert(String sql, String[] keyColumns, Object... params) {
		PreparedStatement stmt = null;
		Object[] keys = null;

        Connection conn = null;
        try {
            conn = jdbcSession.getConnection();
			/*
			 * poor performance. this will ask the db for the meta data
			 */
			// boolean retriveGenKeys = keyColumns != null && conn.getMetaData().supportsGetGeneratedKeys();
		    
			boolean retriveGenKeys = keyColumns != null;
			if (retriveGenKeys)
				stmt = conn.prepareStatement(sql, keyColumns);
			else
				stmt = conn.prepareStatement(sql);

			fillStatement(stmt, params);
			stmt.executeUpdate();

			if (retriveGenKeys) {
			    keys = new Object[keyColumns.length];
				ResultSet rs = stmt.getGeneratedKeys();
				if (rs.next()) {
					for (int i = 0; i < keyColumns.length; i++) {
						keys[i] = rs.getObject(i + 1);
					}
				}
			}

		} catch (SQLException e) {
			rethrow(e, sql, params);

		} finally {
			try {
				close(stmt);
			} catch (SQLException e) {
				throw new PersistenceException(e);
            } finally {
                close(conn);                
			}
		}

		return keys;
	}

	/**
	 * Fill the <code>PreparedStatement</code> replacement parameters with
	 * the given objects.
	 * 
	 * @param stmt
	 *            PreparedStatement to fill
	 * @param params
	 *            Query replacement parameters; <code>null</code> is a valid
	 *            value to pass in.
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public void fillStatement(PreparedStatement stmt, Object... params) throws SQLException {

		if (params == null) {
			return;
		}

		ParameterMetaData pmd = null;
		for (int i = 0; i < params.length; i++) {
			if (params[i] != null) {
				if (params[i] instanceof PreparedStatementCallback)
					((PreparedStatementCallback) params[i]).execute(stmt, i + 1);
				else
					stmt.setObject(i + 1, params[i]);
			} else {
				// throw new SQLException(String.format("Valores nulos como parâmetro não são permitidos. Usar %s", NullSql.class.toString()));

				// VARCHAR works with many drivers regardless
				// of the actual column type. Oddly, NULL and
				// OTHER don't work with Oracle's drivers.
				int sqlType = Types.VARCHAR;
				if (!jdbcSession.getPmdKnownBroken()) {
			        if (pmd == null) {
			            try {
			                pmd = stmt.getParameterMetaData();
			                if (pmd.getParameterCount() < params.length) {
			                    throw new SQLException("Too many parameters: expected "
			                        + pmd.getParameterCount() + ", was given " + params.length);
			                }
			            } catch (SQLException e) {
			                jdbcSession.setPmdKnownBroken(true);
			            }
			        }
				    
					try {
						sqlType = pmd.getParameterType(i + 1);
					} catch (SQLException e) {
                        jdbcSession.setPmdKnownBroken(true);
					}
				}
				stmt.setNull(i + 1, sqlType);
			}
		}
	}

	/**
	 * Close a <code>ResultSet</code>, avoid closing if null.
	 * 
	 * @param rs
	 *            ResultSet to close.
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public void close(ResultSet rs) throws SQLException {
		if (rs != null) {
			rs.close();
		}
	}

	/**
	 * Close a <code>Statement</code>, avoid closing if null.
	 * 
	 * @param stmt
	 *            Statement to close.
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	public void close(Statement stmt) throws SQLException {
		if (stmt != null) {
			stmt.close();
		}
	}

	public void close(Connection conn) {
	    jdbcSession.returnConnection(conn);
    }

	/**
	 * Execute a stored procedure call.<br>
	 * The caller is responsible for closing the connection.
	 * 
	 * @param conn
	 *            The database connection
	 * @param sql
	 *            The JDBC string with call to the procedure
	 * @param parameters
	 *            The list with the parameter values
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
					if (parameter.getValue() != null)
						stmt.setObject(pos, parameter.getValue(), parameter.getJdbcType());
					else
						stmt.setNull(pos, parameter.getJdbcType());
				}
			}

			stmt.execute();

			// obtem primeiro os results set
			pos = 0;
			for (SqlParameter parameter : parameters) {
				if (SqlParameterType.RESULTSET.equals(parameter.getType())) {
					ResultSet rs = (ResultSet) stmt.getObject(parameter.getName());
					IRowTransformer<Object> rt = parameter.getRowTransformer();
					Collection<Object> result = rt.beforeAll(rs);

					try {
						while (rs.next()) {
							rt.onTransformation(result, rt.transform(rs, null));
						}
					} finally {
						rt.afterAll(result);
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
			try {
				close(stmt);
			} catch (SQLException e) {
				throw new PersistenceException(e);
			} finally {
                close(conn);			    
			}
		}

		return outParameters;
	}

	/**
	 * Throws a new exception with a more informative error message.
	 * 
	 * @param cause
	 *            The original exception that will be chained to the new
	 *            exception when it's rethrown.
	 * 
	 * @param sql
	 *            The query that was executing when the exception happened.
	 * 
	 * @param params
	 *            The query replacement parameters; <code>null</code> is a
	 *            valid value to pass in.
	 */
	public void rethrow(SQLException cause, String sql, Object... params) {

		String causeMessage = cause.getMessage();
		if (causeMessage == null) {
			causeMessage = "";
		}
		StringBuffer msg = new StringBuffer(causeMessage);

		msg.append(" Query: ");
		msg.append(sql);
		msg.append(" Parameters: ");

		if (params == null) {
			msg.append("[]");
		} else {
			msg.append(Arrays.deepToString(params));
		}

		if(cause instanceof SQLIntegrityConstraintViolationException){
            throw new PersistenceIntegrityConstraintException(msg.toString(), cause);
		} else {
            throw new PersistenceException(msg.toString(), cause);
		}
	}
}
