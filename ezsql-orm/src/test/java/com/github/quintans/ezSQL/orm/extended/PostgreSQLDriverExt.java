package com.github.quintans.ezSQL.orm.extended;

import com.github.quintans.ezSQL.dml.Function;
import com.github.quintans.ezSQL.driver.EDml;
import com.github.quintans.ezSQL.driver.PostgreSQLDriver;

public class PostgreSQLDriverExt extends PostgreSQLDriver {
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
        return String.format("COALESCE(%s, %s)", translate(dmlType, (Function) o[0]), translate(dmlType, (Function) o[1]));
    }

}