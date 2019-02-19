package com.github.quintans.ezSQL.orm.app.mappings.discriminator;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;

public class TAa {
	public static final Table T_TAA = new Table("TA");
	
	public static final Column<Long> C_ID = T_TAA.BIGINT("ID").AS("id").key();
	public static final Column<String> C_TYPE = T_TAA.VARCHAR("TIPO").AS("tipo");
}
