package com.github.quintans.ezSQL.dml;

/**
 * The purpose of this class is to indicate that the value this is an already converted value.<br>
 * It can be a function or object.
 * Used with subquery, raw, param, ...
 *  
 * @author paulo.quintans
 *
 */
public class FunctionEnd extends Function {
    protected FunctionEnd(String operator, Object value) {
        this.operator = operator;
        this.value = value;
    }
}
