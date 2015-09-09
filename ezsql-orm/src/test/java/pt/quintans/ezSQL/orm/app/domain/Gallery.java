package pt.quintans.ezSQL.orm.app.domain;

import java.util.Set;

public class Gallery extends BaseDomain<Long> {
	
	private String name;
	private String address;
	private Set<Painting> paintings;
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public Set<Painting> getPaintings() {
		return paintings;
	}

	public void setPaintings(Set<Painting> paintings) {
		this.paintings = paintings;
	}

	@Override
	public String toString() {
		final int maxLen = 10;
		StringBuilder builder = new StringBuilder();
		builder.append("Gallery [id=");
		builder.append(id);
		builder.append(", version=");
		builder.append(version);
		builder.append(", name=");
		builder.append(name);
		builder.append(", address=");
		builder.append(address);
		builder.append(", paintings=");
		builder.append(paintings != null ? toString(paintings, maxLen) : null);
		builder.append("]");
		return builder.toString();
	}


}
