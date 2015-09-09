package pt.quintans.ezSQL.orm.app.mappings;

import java.util.Date;

import pt.quintans.ezSQL.db.Column;
import pt.quintans.ezSQL.db.Table;
import pt.quintans.ezSQL.orm.app.domain.EPayGrade;

public class TEmployee {
	public static final Table T_EMPLOYEE = new Table("EMPLOYEE");

	public static final Column<Long> C_ID = T_EMPLOYEE.BIGINT("ID").key();
	public static final Column<String> C_NAME = T_EMPLOYEE.VARCHAR("NAME");
	public static final Column<Boolean> C_SEX = T_EMPLOYEE.BOOLEAN("SEX");
    public static final Column<EPayGrade> C_PAY_GRADE = T_EMPLOYEE.NUMBERED("PAY_GRADE");
	public static final Column<Date> C_CREATION = T_EMPLOYEE.TIMESTAMP("CREATION");
}
