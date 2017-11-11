package com.github.quintans.ezSQL.orm.app.mappings.discriminator;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.db.Table;


public class TCena {
    public static final Table T_CENA = new Table("TA");
    
    public static final Column<Long> C_ID = T_CENA.BIGINT("ID").AS("id").key();
    public static final Column<String> C_TYPE = T_CENA.VARCHAR("TIPO").AS("tipo");
    public static final Column<Long> C_FK = T_CENA.BIGINT("FK").AS("fk");

	static {
        // Discriminators: enable us to give different meanings to the same table. ex: eye color, gender, ...
        T_CENA.WITH(C_TYPE, "B");
    }

}
