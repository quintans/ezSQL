package com.github.quintans.ezSQL.orm.app.domain;

import com.github.quintans.ezSQL.common.api.Updatable;
import com.github.quintans.ezSQL.toolkit.utils.HashCodeUtil;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public abstract class IdentityDomainVO<T extends Serializable> implements Updatable {
    private Set<String> _changed = new HashSet<String>();

	protected T id;

	private int _forHash = 0;

	public IdentityDomainVO(T id) {
		this.id = id;
	}

	public T getId() {
		return this.id;
	}

	public void copy(Object o) {
		if (o instanceof IdentityDomainVO) {
			@SuppressWarnings("unchecked")
            IdentityDomainVO<T> entity = (IdentityDomainVO<T>) o;

			this.id = entity.id;
		}
	}

	@SuppressWarnings("unchecked")
    @Override
	public boolean equals(Object o) {
		if (this == o)
			return true;

		if (o != null && this.getClass().equals(o.getClass())) {
			IdentityDomainVO<T> be = (IdentityDomainVO<T>) o;
			return this.id != null && this.id.equals(be.id);
		} else
			return false;
	}

	@Override
	public int hashCode() {
		if (this._forHash == 0) {
			int result = HashCodeUtil.SEED;
			result = HashCodeUtil.hash(result, this.getClass());
			result = HashCodeUtil.hash(result, this.id);
			this._forHash = result;
		}

		return this._forHash;
	}

	protected String toString(Collection<?> collection, int maxLen) {
		StringBuilder builder = new StringBuilder();
		builder.append("[");
		int i = 0;
		for (Iterator<?> iterator = collection.iterator(); iterator.hasNext()
			&& i < maxLen; i++) {
			if (i > 0)
				builder.append(", ");
			builder.append(iterator.next());
		}
		builder.append("]");
		return builder.toString();
	}

    @Override
    public Set<String> changed() {
        if(!_changed.isEmpty())
            return _changed;
        else
            return null;
    }

    @Override
    public void clear() {
        _changed.clear();
    }
    
    protected void dirty(String name){
        _changed.add(name);
    }

}
