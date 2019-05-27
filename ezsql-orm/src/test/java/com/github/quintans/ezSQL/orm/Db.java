package com.github.quintans.ezSQL.orm;

import java.sql.Connection;

import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.Delete;
import com.github.quintans.ezSQL.dml.CoreDSL;
import com.github.quintans.ezSQL.dml.Insert;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.dml.Update;
import com.github.quintans.ezSQL.driver.Driver;

public class Db extends com.github.quintans.ezSQL.Db {
	// for testing
	public String language = "pt";
	public static final String LANG_PARAMETER = "language";

	public Db(Driver driver, Connection connection) {
		super(driver, connection);
	}

	@Override
	public Query query(Table table) {
		Query query = super.query(table);
		setEnvironment(query);
		return query;
	}

	public Query queryAll(Table table) {
		Query query = new Query(this, table).all();
		setEnvironment(query);
		return query;
	}

	@Override
	public Insert insert(Table table) {
		Insert insert = super.insert(table);
		setEnvironment(insert);
		return insert;
	}

	@Override
	public Update update(Table table) {
		Update update = super.update(table);
		setEnvironment(update);
		return update;
	}

	@Override
	public Delete delete(Table table) {
		Delete delete = super.delete(table);
		setEnvironment(delete);
		return delete;
	}

	private void setEnvironment(CoreDSL dml) {
		dml.setParameter(LANG_PARAMETER, this.language);
	}

}
