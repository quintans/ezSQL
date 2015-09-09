package pt.quintans.ezSQL.orm.app.mappings.virtual;

import pt.quintans.ezSQL.db.Association;
import pt.quintans.ezSQL.db.Column;
import pt.quintans.ezSQL.db.Table;
import pt.quintans.ezSQL.orm.Db;
import static pt.quintans.ezSQL.dml.Definition.param;
import static pt.quintans.ezSQL.orm.app.mappings.virtual.TAuthor.*;

public class TBook {
	public static final Table T_BOOK = new Table("BOOK");

	public static final Column<Long> C_ID = T_BOOK.BIGINT("ID").key();
	public static final Column<Integer> C_VERSION = T_BOOK.INTEGER("VERSION").version();
	public static final Column<Long> C_AUTHOR = T_BOOK.BIGINT("AUTHOR_ID").AS("authorFk");
	public static final Column<Double> C_PRICE = T_BOOK.DECIMAL("PRICE");

	// association with discriminator
	public final static Association A_I18N = T_BOOK
	        .ASSOCIATE(C_ID)
	        .TO(TBook18.C_ID)
	        .AS("i18n")
	        .WITH(TBook18.C_LANG, param(Db.LANG_PARAMETER));

	public final static Association A_AUTHOR = T_BOOK
	        .ASSOCIATE(C_AUTHOR).TO(T_AUTHOR.C_ID).AS("author"); 
}
