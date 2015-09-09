package pt.quintans.ezSQL.orm.app.mappings.discriminator;

import pt.quintans.ezSQL.db.Column;
import pt.quintans.ezSQL.db.Table;

public class TAa {
	public static final Table T_TAA = new Table("TA");
	
	public static final Column<Long> C_ID = T_TAA.BIGINT("ID").AS("id").key();
	public static final Column<String> C_TYPE = T_TAA.VARCHAR("TIPO").AS("tipo");
	public static final Column<Long> C_FK = T_TAA.BIGINT("FK").AS("fk");
}
