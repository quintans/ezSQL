package com.github.quintans.ezSQL.orm.app.mappings.virtual;

import com.github.quintans.ezSQL.db.*;

public class TAuthor extends Table {
	public static final TAuthor T_AUTHOR = new TAuthor();
	
	protected TAuthor() {
	    super("AUTHOR");
	}

	public final Column<Long> C_ID = BIGINT("ID").key();
	public final Column<Integer> C_VERSION = INTEGER("VERSION").version();
	public final Column<String> C_NAME = VARCHAR("NAME");

	// ONE author has MANY books
	public final Association A_BOOKS = ASSOCIATE(C_ID).TO(TBook.C_AUTHOR).AS("books");
}
