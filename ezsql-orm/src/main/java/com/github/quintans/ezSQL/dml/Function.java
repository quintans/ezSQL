package com.github.quintans.ezSQL.dml;

import static com.github.quintans.ezSQL.toolkit.utils.Misc.match;

import com.github.quintans.ezSQL.Base;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.exceptions.PersistenceException;
import com.github.quintans.ezSQL.toolkit.utils.HashCodeUtil;

public class Function extends Base<Object> {
	protected String operator;
	protected Function[] members;
	protected String alias;
    protected String tableAlias;
    protected String pseudoTableAlias;
    
    protected Object value;
    
	protected Function(){}
	
	protected Function(String operator, Object... members) {
		this.operator = operator;
        this.members = converteAll(members);
	}

    public String getOperator() {
		return this.operator;
	}

    protected void setOperator(String operator) {
		this.operator = operator;
	}

	protected void setMembers(Function... members) {
		this.members = members;
	}

	public Function[] getMembers() {
		return this.members;
	}

	public Object getValue() {
        return value;
    }

	protected void setValue(Object value) {
        this.value = value;
    }
    
    public String getAlias() {
		return this.alias;
	}

	public void as(String alias) {
		this.alias = alias;
	}
	
    // Propagates table alias
	protected void setTableAlias(String tableAlias) {
		if (this.members != null) {
			for (Object o : this.members) {
				// pode conter outros objectos. ex: param("lixo")
				if (o instanceof Function) {
					Function func = (Function) o;
					func.setTableAlias(tableAlias);
				}
			}
		}
	}
	
	public String getTableAlias() {
        return tableAlias;
    }

    public String getPseudoTableAlias() {
        return pseudoTableAlias != null ? pseudoTableAlias : tableAlias;
    }

    protected void setPseudoTableAlias(String pseudoTableAlias) {
        this.pseudoTableAlias = pseudoTableAlias;
    }

    @Override
    public Object clone() {
        return clone(new Function());
    }

    protected Function clone(Function function) {
		function.setOperator(this.operator);
		function.as(this.alias);

		if (this.members != null) {
		    Function[] otherMembers = new Function[this.members.length];
			for (int i = 0; i < this.members.length; i++) {
				// pode conter outros objectos. ex: param("lixo")
			    otherMembers[i] = (Function) this.members[i].clone();
			}
			function.setMembers(otherMembers);
		}
		else if (this.value instanceof Function) {
		    function.value = (((Function) this.value).clone());
		} else {
	          function.value = this.value;
		}
		
		return function;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{operator=").append(this.operator).append(", ");
        sb.append("members=");
		if (this.members != null) {
	        boolean comma = false;
			sb.append("[");
			for (Object o : this.members) {
				if (comma)
					sb.append("; ");
				sb.append(o);
				comma = true;
			}
		} else {
	        sb.append("null");
		}
        sb.append(", value=").append(this.value);
		sb.append(", alias=").append(this.alias).append("]}");

		return sb.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (o == null)
			return false;

		if (o == this)
			return true;

		if (o instanceof Function) {
			Function other = (Function) o;
			if (this.operator.equals(other.operator) 
			        && match(this.alias, other.alias)
			        && match(this.value, other.value) 
			        && matchMembers(other.members)) {
				return true;
			}
		}

		return false;
	}

	private boolean matchMembers(Function... m) {
		if (this.members == m)
			return true;
		else if (this.members == null || m == null || this.members.length != m.length)
			return false;

		int idx = 0;
		for (Object o : this.members) {
			if (!match(o, m[idx]))
				return false;
			idx++;
		}

		return true;
	}

	private int _forHash = 0;

	@Override
	public int hashCode() {
		if (this._forHash == 0) {
			int result = HashCodeUtil.SEED;
			result = HashCodeUtil.hash(result, this.getClass());
			result = HashCodeUtil.hash(result, this.operator);
			result = HashCodeUtil.hash(result, this.alias);
			result = HashCodeUtil.hash(result, this.members);
			this._forHash = result;
		}

		return this._forHash;
	}
	
    public static Function converteOne(Object value) {
        if(value == null) {
            throw new PersistenceException("Value cannot be NULL. Use one of NullSql types.");
        } if (value instanceof Column) {
            return new ColumnHolder((Column<?>) value);
        }
        else if (value instanceof Function) {
            return (Function) ((Function) value).clone();
        }
        else if (value instanceof Query) {
            return Definition.subQuery((Query) value);
        }
        else {
            return new FunctionEnd(EFunction.RAW, value);
        }
    }

    public static Function[] converteAll(Object[] values) {
        Function[] tokens = new Function[values.length];
        for (int k = 0; k < values.length; k++) {
            tokens[k] = converteOne(values[k]);
        }
        return tokens;
    }
}
