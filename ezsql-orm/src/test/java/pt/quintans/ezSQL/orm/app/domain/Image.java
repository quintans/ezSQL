package pt.quintans.ezSQL.orm.app.domain;

import pt.quintans.ezSQL.toolkit.io.BinStore;

public class Image extends BaseDomain<Long> {
	private BinStore content;

	public BinStore getContent() {
		return this.content;
	}

	public void setContent(BinStore content) {
		this.content = content;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Image [id=");
		builder.append(this.id);
		builder.append(", version=");
		builder.append(this.version);
		builder.append(", content=[bin]");
		builder.append("]");
		return builder.toString();
	}

}
