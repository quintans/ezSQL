package com.github.quintans.ezSQL.orm.app.mappings.discriminator;

import com.github.quintans.ezSQL.db.Association;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;

public class TMain {	
    public static final Table T_MAIN = new Table("TA");
    
    public static final Column<Long> C_ID = T_MAIN.BIGINT("ID").AS("id").key();
    public static final Column<String> C_TYPE = T_MAIN.VARCHAR("TIPO").AS("tipo");
    public static final Column<Long> C_FK = T_MAIN.BIGINT("FK").AS("fk");

    public static final String D_BE = "B"; // also used at the other side
	public static final Association A_BE = T_MAIN 
	        .ASSOCIATE(C_FK)
	        .TO(TBe.C_ID)
	        .AS("be")
            .WITH(TAa.C_TYPE, D_BE);

    public static final String D_CE = "C";  // also used at the other side
	public static final Association A_CE =  T_MAIN
            .ASSOCIATE(C_FK).TO(TCe.C_ID).AS("ce")
            .WITH(TAa.C_TYPE, D_CE);
}
