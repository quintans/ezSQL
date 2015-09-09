package pt.quintans.ezSQL.orm.app.mappings.virtual;

import pt.quintans.ezSQL.db.Column;
import pt.quintans.ezSQL.db.Table;

/**
 * Example of a discriminators table. This example redeclares the table CATALOG, while the  Another way is TEyecolor.
 * 
 * @author paulo.quintans
 *
 */
public class TGender {
	public static final Table T_GENDER = new Table("CATALOG").AS("gender");

	public static final Column<Long> C_ID = T_GENDER.BIGINT("ID").key();
	/*
	 * Discriminators: enable us to give different meanings to the same table. ex: eye color, gender, ...
	 */
	public static final Column<String> C_TYPE = T_GENDER.VARCHAR("KIND").WITH("GENDER");
	public static final Column<String> C_KEY = T_GENDER.VARCHAR("TOKEN");
	public static final Column<String> C_VALUE = T_GENDER.VARCHAR("VALUE").AS("name");
}
