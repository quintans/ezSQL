package com.github.quintans.ezSQL.orm;

import java.sql.Connection;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.db.Table;
import com.github.quintans.ezSQL.dml.Delete;
import com.github.quintans.ezSQL.dml.DmlBase;
import com.github.quintans.ezSQL.dml.Insert;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.dml.Update;

public class Db extends AbstractDb {
	// for testing
	public String languague = "pt";
	public static final String LANG_PARAMETER = "language";

	private Connection connection = null;

	public Db(Connection connection) {
		super();
		this.connection = connection;
	}

	@Override
	protected Connection connection() {
		return this.connection;
	}

    @Override
    protected void releaseConnection(Connection connection) {
    }

	public void setConnection(Connection connection) {
		this.connection = connection;
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

	private void setEnvironment(DmlBase dml) {
		dml.setParameter(LANG_PARAMETER, this.languague);
	}

}
