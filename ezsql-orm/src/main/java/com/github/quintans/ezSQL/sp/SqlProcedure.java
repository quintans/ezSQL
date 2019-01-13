package com.github.quintans.ezSQL.sp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.dml.Dml;
import com.github.quintans.jdbc.SimpleJdbc;
import com.github.quintans.jdbc.sp.SqlParameter;

/**
 * Definition of a Database Stored Procedure
 * 
 * @author paulo.quintans
 * 
 */
public class SqlProcedure {
	private static org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(SqlProcedure.class);
	private static final String FQCN = Dml.class.getCanonicalName();

	private AbstractDb db;
    protected SimpleJdbc simpleJdbc;

	/**
	 * the return type. If defined it is a Function.
	 */
	private SqlParameter returnType;
	/**
	 * The database stored procedure name
	 */
	private String name = "";
	/**
	 * list of parameters of this procedure
	 */
	private List<SqlParameter> parameters;

	/**
	 * Stored procedure constructor
	 * 
	 * @param db connection wrapper
	 * 
	 * @param name
	 *            name of the procedure
	 * @param params
	 *            list o parameter for this procedure
	 */
	/**
	 * 
	 * @param db connection wrapper
	 * @param name
	 * @param params
	 */
	public SqlProcedure(AbstractDb db, String name, SqlParameter... params) {
		this(db, null, name, params);
	}

	/**
	 * Stored function constructor. In this case it is a Function, as we are declaring a return type
	 * 
	 * @param db connection wrapper
	 * @param returnType
	 *            function return type
	 * @param name
	 *            function name
	 * @param params
	 *            list o parameter for this function
	 */
	public SqlProcedure(AbstractDb db, SqlParameter returnType, String name, SqlParameter... params) {
		this.db = db;
        this.simpleJdbc = new SimpleJdbc(db.getJdbcSession());
		
		this.returnType = returnType;
		this.name = name;

		this.parameters = new ArrayList<SqlParameter>();
		if (this.parameters != null) {
			for (SqlParameter parameter : params)
				this.parameters.add(parameter);
		}
	}

	/**
	 * getter for the return type
	 * 
	 * @return
	 */
	public SqlParameter getReturnType() {
		return this.returnType;
	}

	/**
	 * getter for the name
	 * 
	 * @return
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * getter for the parameters
	 * 
	 * @return
	 */
	public List<SqlParameter> getParameters() {
		return this.parameters;
	}

	/**
	 * checks if it is a function
	 * 
	 * @return
	 */
	public boolean isFunction() {
		return this.returnType != null;
	}

	/**
	 * Executes a procedure
	 * 
	 * @return map with the result. The result mapping was previously defined when constructing the <code>SqlProcedure</code>
	 */
	public Map<String, Object> call() {
		return call(new HashMap<String, Object>());
	}

	public Map<String, Object> call(Map<String, Object> values) {
		List<SqlParameter> copies = new ArrayList<SqlParameter>();
		if (isFunction())
			copies.add(new SqlParameter(getReturnType()));

		for (SqlParameter parameter : getParameters()) {
			SqlParameter copy = new SqlParameter(parameter);
			copies.add(copy);
			if (parameter.isIn() && values.containsKey(parameter.getName()))
				copy.setValue(values.get(parameter.getName()));
		}

		String sql = this.db.getDriver().getSql(this);
		debugSQL(sql, copies);

		long now = System.nanoTime();
		Map<String, Object> response = simpleJdbc.call(sql, copies);
		debugTime(now);

		return response;
	}

	private String dumpParameters(List<SqlParameter> parameters) {
		StringBuilder sb = new StringBuilder();

		for (SqlParameter parameter : parameters) {
			if (parameter.isIn()) {
				if (parameter.getValue() != null)
					sb.append(String.format("[%s:%s=%s]", parameter.getType(), parameter.getName(), parameter.getValue().toString()));
				else
					sb.append(String.format("[%s:%s=NULL]", parameter.getType(), parameter.getName()));
			} else
				sb.append(String.format("[%s:%s]", parameter.getType(), parameter.getName()));
		}

		return sb.toString();
	}

	private void debugTime(long now) {
		if (LOG.isDebugEnabled()) {
			LOG.log(FQCN, Level.DEBUG,
				"executed in: " + (System.nanoTime() - now)/1e6 + "ms",
				null);
		}
	}

	private void debugSQL(String sql, List<SqlParameter> copies) {
		if (LOG.isDebugEnabled()) {
			LOG.log(FQCN, Level.DEBUG,
				String.format("\n\tSQL: %s\n\tparameters: %s", sql, dumpParameters(copies)),
				null);
		}
	}
}
