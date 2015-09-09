package pt.quintans.ezSQL.orm.app.mappings.virtual;

import pt.quintans.ezSQL.db.Column;
import pt.quintans.ezSQL.db.Table;

public class TCatalog extends Table {
	public static final TCatalog T_CATALOG = new TCatalog();
	
	protected TCatalog(){
	    super("CATALOG");
	}
	
	public final Column<Long> C_ID = BIGINT("ID").key();

	public final Column<String> C_TYPE = VARCHAR("KIND");
	public final Column<String> C_KEY = VARCHAR("TOKEN");
	public final Column<String> C_VALUE = VARCHAR("VALUE").AS("name");
}
