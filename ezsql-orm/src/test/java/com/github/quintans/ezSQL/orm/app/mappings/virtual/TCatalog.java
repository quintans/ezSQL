package com.github.quintans.ezSQL.orm.app.mappings.virtual;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;

public class TCatalog extends Table {
	public static final TCatalog T_CATALOG = new TCatalog();
	
	protected TCatalog(){
	    super("CATALOG");
	}
	
	public final Column<Long> C_ID = BIGINT("ID").key(Long.class);

	public final Column<String> C_TYPE = VARCHAR("KIND");
	public final Column<String> C_KEY = VARCHAR("TOKEN");
	public final Column<String> C_VALUE = VARCHAR("VALUE").AS("name");
}
