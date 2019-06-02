package com.github.quintans.ezSQL.orm.app.mappings.virtual;

public class Catalog {
	private String token;
	private String name;

	public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Gender [token=");
		builder.append(this.token);
		builder.append(", name=");
		builder.append(this.name);
		builder.append("]");
		return builder.toString();
	}

}
