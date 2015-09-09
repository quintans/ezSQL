package pt.quintans.ezSQL.orm.app.mappings;

import java.util.Date;

import pt.quintans.ezSQL.common.type.MyDateTime;
import pt.quintans.ezSQL.db.Column;
import pt.quintans.ezSQL.db.Table;

public class TTimer {
	public static final Table T_TIMER = new Table("TIMER");

	public static final Column<Long> C_ID = T_TIMER.BIGINT("ID").key();
	public static final Column<MyDateTime> C_DATE = T_TIMER.DATETIME("MY_DATE").AS("date");
	public static final Column<Date> C_TIMESTAMP = T_TIMER.TIMESTAMP("MY_TS").AS("ts");
}
