package pt.quintans.ezSQL.orm.app.mappings.discriminator;

import pt.quintans.ezSQL.db.Association;
import pt.quintans.ezSQL.db.Column;
import pt.quintans.ezSQL.db.Table;

public class TCe {
	public static final Table meta = new Table("TC").AS("ce");

	public static final Column<Long> C_ID = meta.BIGINT("ID").key();
	public static final Column<String> C_DSC = meta.VARCHAR("DSC");
    
	public final static Association A_MAIN = meta
            .ASSOCIATE(C_ID).TO(TMain.C_FK).AS("mains")
            .WITH(TMain.C_TYPE, TMain.D_CE);
}
