package com.github.quintans.ezSQL.orm.app.mappings.discriminator;

import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;

public class TBe {
	public static final Table T_BE = new Table("TB").AS("be");

	public static final Column<Long> C_ID = T_BE.BIGINT("ID").AS("id").key(Long.class);
	public static final Column<String> C_DSC = T_BE.VARCHAR("DSC").AS("dsc");
	public static final Column<Long> C_FK = T_BE.BIGINT("FK").AS("fk");

	public final static Association A_MAIN = T_BE
	        .ASSOCIATE(C_FK).TO(TMain.C_ID).AS("mains")
	        .WITH(TMain.C_TYPE, TMain.D_BE);
}
