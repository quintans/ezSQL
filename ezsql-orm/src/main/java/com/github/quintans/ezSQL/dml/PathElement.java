package com.github.quintans.ezSQL.dml;

import java.util.List;

import com.github.quintans.ezSQL.db.Association;

public class PathElement {
	private Boolean inner;
	private Association base;
	private Association derived;
	private Condition condition;
	private List<Function> columns;
	private List<Sort> orders;
	private String preferredAlias; // user preferred alias

	public PathElement(Association base, Boolean inner) {
		this.base = base;
		this.inner = inner;
	}

	public Association getBase() {
		return this.base;
	}

	public void setBase(Association base) {
		this.base = base;
	}

	public Association getDerived() {
		return this.derived;
	}

	public void setDerived(Association derived) {
		this.derived = derived;
	}

	public Boolean isInner() {
		return this.inner;
	}

	public void setInner(boolean inner) {
		this.inner = inner;
	}

	public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    public List<Function> getColumns() {
        return columns;
    }

    public void setColumns(List<Function> columns) {
        this.columns = columns;
    }

    public List<Sort> getOrders() {
        return orders;
    }

    public void setOrders(List<Sort> orders) {
        this.orders = orders;
    }
    
    public String getPreferredAlias() {
		return preferredAlias;
	}

	public void setPreferredAlias(String preferredAlias) {
		this.preferredAlias = preferredAlias;
	}

	@Override
	public Object clone() {
		PathElement pe = new PathElement(this.base, this.inner);
		pe.setDerived(this.derived);
		pe.setCondition(condition);
		pe.setColumns(columns);
		pe.setOrders(orders);
		pe.setPreferredAlias(preferredAlias);		
		return pe;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("PathElement [base=");
		builder.append(this.base);
		builder.append(", derived=");
		builder.append(this.derived);
		builder.append(", inner=");
		builder.append(this.inner);
		builder.append(", preferredAlias=");
		builder.append(this.preferredAlias);
		builder.append("]");
		return builder.toString();
	}

}
