package com.github.quintans.ezSQL.orm.extended;

import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.translator.EDml;
import com.github.quintans.ezSQL.translator.H2Translator;

public class H2TranslatorExt extends H2Translator {

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
