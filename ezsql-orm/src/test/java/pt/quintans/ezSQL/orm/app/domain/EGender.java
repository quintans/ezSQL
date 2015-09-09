package pt.quintans.ezSQL.orm.app.domain;

import pt.quintans.ezSQL.common.api.Value;

public enum EGender implements Value<String> {
	MALE("M"),
	FEMALE("F");

	private String value;

	private EGender(String value) {
		this.value = value;
	}

	@Override
	public String value() {
		return this.value;
	}
}
