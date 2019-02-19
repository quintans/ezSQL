package com.github.quintans.ezSQL;

import java.sql.Connection;

public class Db extends AbstractDb {

	private Connection connection;

	public Db(Connection connection) {
		super();
		this.connection = connection;
	}

	@Override
	protected Connection connection() {
		return this.connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}
	
}