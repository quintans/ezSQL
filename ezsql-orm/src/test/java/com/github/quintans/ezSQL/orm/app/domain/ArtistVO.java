package com.github.quintans.ezSQL.orm.app.domain;

import java.util.Date;
import java.util.Set;

public class Artist extends BaseDomain<Long> {
	private String name;
	private Set<Painting> paintings;
	private EGender gender;
	private Date birthday;

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
        dirty("name");
		this.name = name;
	}

	public Set<Painting> getPaintings() {
		return this.paintings;
	}

	public void setPaintings(Set<Painting> paintings) {
		this.paintings = paintings;
	}

	public EGender getGender() {
		return this.gender;
	}

	public void setGender(EGender gender) {
        dirty("gender");
		this.gender = gender;
	}
	
	public Date getBirthday() {
        return birthday;
    }

    public void setBirthday(Date birthday) {
        dirty("birthday");
        this.birthday = birthday;
    }

    @Override
	public String toString() {
		final int maxLen = 10;
		StringBuilder builder = new StringBuilder();
		builder.append("Artist [id=").append(this.id);
		builder.append(", version=").append(this.version);
		builder.append(", name=").append(this.name);
		builder.append(", paintings=");
		builder.append(this.paintings != null ? toString(this.paintings, maxLen) : null);
		builder.append(", gender=").append(this.gender);
        builder.append(", birthday=").append(this.birthday);
		builder.append("]");
		return builder.toString();
	}

}
