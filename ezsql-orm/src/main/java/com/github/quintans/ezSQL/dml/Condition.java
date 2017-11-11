package com.github.quintans.ezSQL.dml;


/*
 * Objectivo: marcar as funcoes que sao condition
 */
public class Condition extends Function {
    private boolean not = false;

    public Condition(String operator, Object... members) {
        super(operator, members);
    }

    public Condition not() {
        this.not = true;
        return this;
    }

    public boolean isNot() {
        return this.not;
    }

    @Override
    public Object clone() {
        Condition c = new Condition(getOperator());
        clone(c);

        if (this.not)
            c.not();
        return c;
    }

    public Condition and(Condition condition) {
        return Definition.and(this, condition);
    }

    public Condition or(Condition condition) {
        return Definition.or(this, condition);
    }
}
