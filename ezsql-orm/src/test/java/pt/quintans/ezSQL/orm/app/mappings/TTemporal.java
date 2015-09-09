package pt.quintans.ezSQL.orm.app.mappings;

import java.util.Date;

import pt.quintans.ezSQL.common.type.MyDate;
import pt.quintans.ezSQL.common.type.MyTime;
import pt.quintans.ezSQL.db.*;

public class TTemporal {
	public static final Table T_TEMPORAL = new Table("TEMPORAL");

	public static final Column<Long> C_ID = T_TEMPORAL.BIGINT("ID").key();

    public static final Column<MyTime> C_CLOCK = T_TEMPORAL.TIME("CLOCK");
	public static final Column<MyDate> C_TODAY = T_TEMPORAL.DATE("CALENDAR").AS("today");
	// special case: timestamp on local zone
    public static final Column<Date> C_NOW = T_TEMPORAL.TIMESTAMP("NOW");
	public static final Column<Date> C_INSTANT = T_TEMPORAL.TIMESTAMP("INSTANT");
}
