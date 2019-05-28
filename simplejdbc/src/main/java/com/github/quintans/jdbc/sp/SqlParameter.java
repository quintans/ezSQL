package com.github.quintans.jdbc.sp;

import java.sql.Types;

import com.github.quintans.jdbc.transformers.IResultTransformer;

/**
 * SQL parameter used in calling a stored procedure
 * 
 * @author paulo.quintans
 * 
 */
public class SqlParameter {
	private boolean defined;
	private SqlParameterType type;
	private String name;
	private Object value;
	private int jdbcType;
	private IResultTransformer<Object> rowTransformer;

	/**
	 * copy constructor
	 * 
	 * @param copy
	 */
	public SqlParameter(SqlParameter copy) {
		this.type = copy.getType();
		this.name = copy.getName();
		this.jdbcType = copy.getJdbcType();
		this.value = copy.getValue();
		this.rowTransformer = copy.getRowTransformer();
		this.defined = copy.isDefined();
	}

	/**
	 * Standard constructor
	 * 
	 * @param type
	 *            type of a SqlParameter
	 * @param name
	 *            name of the parameter
	 * @param jdbcType
	 *            JDBC type
	 */
	public SqlParameter(SqlParameterType type, String name, int jdbcType) {
		this.type = type;
		this.name = name;
		this.jdbcType = jdbcType;
	}

	/**
	 * Constructor for a OUT parameter that is a ResultSet
	 * 
	 * @param name
	 *            name of the parameter
	 * @param rowTransformer
	 *            the result transformer
	 */
	public SqlParameter(String name, IResultTransformer<Object> rowTransformer) {
		this.type = SqlParameterType.RESULTSET;
		this.name = name;
		this.rowTransformer = rowTransformer;
		this.jdbcType = Types.OTHER;
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
	 * gets if the value for this parameter was set
	 * 
	 * @return
	 */
	public boolean isDefined() {
		return this.defined;
	}

	/**
	 * getter for the value
	 * 
	 * @return
	 */
	public Object getValue() {
		return this.value;
	}

	/**
	 * setter for the value
	 * 
	 * @param value
	 */
	public void setValue(Object value) {
		this.defined = true;
		this.value = value;
	}

	/**
	 * getter for the type
	 * 
	 * @return
	 */
	public SqlParameterType getType() {
		return this.type;
	}

	/**
	 * getter for the jdbcType
	 * 
	 * @return
	 */
	public int getJdbcType() {
		return this.jdbcType;
	}

	/**
	 * checks if the parameter is for input
	 * 
	 * @return
	 */
	public boolean isIn() {
		return SqlParameterType.IN.equals(this.type) || SqlParameterType.IN_OUT.equals(this.type);
	}

	/**
	 * checks if the parameter is for output
	 * 
	 * @return
	 */
	public boolean isOut() {
		return SqlParameterType.OUT.equals(this.type) || SqlParameterType.IN_OUT.equals(this.type) || SqlParameterType.RESULTSET.equals(this.type);
	}

	/**
	 * getter for the <code>RowTransformer</code>
	 * 
	 * @return
	 */
	public IResultTransformer<Object> getRowTransformer() {
		return this.rowTransformer;
	}

	/**
	 * creates a <code>SqlParameter.IN</code> parameter
	 * 
	 * @param name
	 *            name parameter
	 * @param jdbcType
	 *            jdbc type
	 * @return the parameter
	 */
	public static SqlParameter IN(String name, int jdbcType) {
		return new SqlParameter(SqlParameterType.IN, name, jdbcType);
	}

	/**
	 * creates a <code>SqlParameter.OUT</code> parameter
	 * 
	 * @param name
	 *            name parameter
	 * @param jdbcType
	 *            jdbc type
	 * @return the parameter
	 */
	public static SqlParameter OUT(String name, int jdbcType) {
		return new SqlParameter(SqlParameterType.OUT, name, jdbcType);
	}

	/**
	 * creates a <code>SqlParameter.IN_OUT</code> parameter
	 * 
	 * @param name
	 *            name parameter
	 * @param jdbcType
	 *            jdbc type
	 * @return the parameter
	 */
	public static SqlParameter IN_OUT(String name, int jdbcType) {
		return new SqlParameter(SqlParameterType.IN_OUT, name, jdbcType);
	}

	/**
	 * creates a <code>SqlParameter.RESULTSET</code> parameter
	 * 
	 * @param name
	 *            name parameter
	 * @param rowTransformer
	 *            the transformer
	 * @return
	 */
	public static SqlParameter RESULTSET(String name, IResultTransformer<Object> rowTransformer) {
		return new SqlParameter(name, rowTransformer);
	}

	@Override
	public String toString() {
		return String.format("SqlParameter {type: %s, name: %s, jdbcType: %s, value: %s}", this.type, this.name, this.jdbcType, this.value);
	}
}
