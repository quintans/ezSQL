package pt.quintans.ezSQL.orm.app.mappings.discriminator;

import pt.quintans.ezSQL.db.Association;
import pt.quintans.ezSQL.db.Column;
import pt.quintans.ezSQL.db.Table;

public class TThing {
	public static final Table T_THING = new Table("TC");

	public static final Column<Long> C_ID = T_THING.BIGINT("ID").key();
	public static final Column<String> C_DSC = T_THING.VARCHAR("DSC");
    
	public final static Association A_TAA_B = T_THING
            .ASSOCIATE(C_ID).TO(TCena.C_FK).AS("cenas");
}
