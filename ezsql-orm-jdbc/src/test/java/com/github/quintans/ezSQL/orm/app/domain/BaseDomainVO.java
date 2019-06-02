package com.github.quintans.ezSQL.orm.app.domain;

import java.io.Serializable;


public abstract class BaseDomainVO<T extends Serializable> extends IdentityDomainVO<T> {

	protected Integer version;

	public BaseDomainVO(T id, Integer version) {
		super(id);
		this.version = version;
	}

	public boolean persited() {
		return version != null;
	}

	public Integer getVersion() {
		return this.version;
	}

	@SuppressWarnings("unchecked")
    public void copy(Object o) {
	    super.copy(o);
		if (o instanceof BaseDomainVO) {
			BaseDomainVO<T> entity = (BaseDomainVO<T>) o;
			this.version = entity.version;
		}
	}

}
