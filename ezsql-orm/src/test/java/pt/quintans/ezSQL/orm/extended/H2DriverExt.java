package pt.quintans.ezSQL.orm.extended;

import pt.quintans.ezSQL.dml.Function;
import pt.quintans.ezSQL.driver.EDml;
import pt.quintans.ezSQL.driver.H2Driver;

public class H2DriverExt extends H2Driver {

	@Override
	public String translate(EDml dmlType, Function function) {
		String op = function.getOperator();
		if(FunctionExt.IFNULL.equals(op)){
			return ifNull(dmlType, function);
		} else
			return super.translate(dmlType, function);
	}
	
	public String ifNull(EDml dmlType, Function function) {
		Object[] o = function.getMembers();
		return String.format("IFNULL(%s, %s)", translate(dmlType, (Function) o[0]), translate(dmlType, (Function) o[1]));
	}
}
