package com.github.quintans.ezSQL;

import com.github.quintans.ezSQL.driver.Driver;

import java.sql.Connection;

public class Db extends AbstractDb {

	private Connection connection;

	public Db(Driver driver, Connection connection) {
		super(driver);
		this.connection = connection;
	}

	@Override
	protected Connection connection() {
		return this.connection;
	}
	
}