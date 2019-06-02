package com.github.quintans.ezSQL.orm.app.mappings.discriminator;

import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;

public class TCe {
	public static final Table T_CE = new Table("TC").AS("ce");

	public static final Column<Long> C_ID = T_CE.BIGINT("ID").key(Long.class);
	public static final Column<String> C_DSC = T_CE.VARCHAR("DSC");
	public static final Column<Long> C_FK = T_CE.BIGINT("FK").AS("fk");
    
	public final static Association A_MAIN = T_CE
            .ASSOCIATE(C_FK).TO(TMain.C_ID).AS("mains")
            .WITH(TMain.C_TYPE, TMain.D_CE);
}
