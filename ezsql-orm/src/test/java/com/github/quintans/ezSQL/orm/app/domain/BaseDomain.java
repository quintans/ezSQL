package com.github.quintans.ezSQL.orm.app.domain;

import java.io.Serializable;


public abstract class BaseDomain<T extends Serializable> extends IdentityDomain<T> {

	protected Integer version = 0;

	public BaseDomain() {
	}

	public boolean persisted() {
		return version != null && !version.equals(0);
	}

	public Integer getVersion() {
		return this.version;
	}

	public void setVersion(Integer version) {
		this.version = version;
	}

	@SuppressWarnings("unchecked")
    public void copy(Object o) {
	    super.copy(o);
		if (o instanceof BaseDomain) {
			BaseDomain<T> entity = (BaseDomain<T>) o;
			this.version = entity.version;
		}
	}

}
