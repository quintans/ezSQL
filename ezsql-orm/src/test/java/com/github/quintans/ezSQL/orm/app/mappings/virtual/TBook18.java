package com.github.quintans.ezSQL.orm.app.mappings.virtual;

import com.github.quintans.ezSQL.db.*;

public class TBook18 {
	public static final Table T_BOOK18 = new Table("BOOK_I18N").AS("book18");

	public static final Column<Long> C_ID = T_BOOK18.BIGINT("ID").key(Long.class);
	public static final Column<String> C_LANG = T_BOOK18.VARCHAR("LANG").key(String.class);
	public static final Column<String> C_NAME = T_BOOK18.VARCHAR("NAME");

	// this association should never be used??
	// public final static Association A_BOOK = new Association(
	// "book",
	// C_LANG.matches(param("language")),
	// new Relation(C_ID, TBook.C_ID)
	// );
}
