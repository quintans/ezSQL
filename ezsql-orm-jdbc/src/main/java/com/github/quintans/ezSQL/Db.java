package com.github.quintans.ezSQL;

import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.translator.Translator;

import java.sql.Connection;

public class Db extends AbstractDb {

	private Connection connection;

	public Db(Translator translator, Driver driver, Connection connection) {
		super(translator, driver);
		this.connection = connection;
	}

	@Override
	public Connection getConnection() {
		return this.connection;
	}
	
}