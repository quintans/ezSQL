package pt.quintans.ezSQL.orm.app.dtos;

import pt.quintans.ezSQL.orm.app.domain.BaseDomain;

/**
 * This class is the same as Image, but uses byte[] instead of ByteCache
 * 
 * @author paulo.quintans
 *
 */
public class ImageDTO extends BaseDomain<Long> {
	private byte[] content;

	public byte[] getContent() {
		return this.content;
	}

	public void setContent(byte[] content) {
		this.content = content;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Image [id=");
		builder.append(this.id);
		builder.append(", version=");
		builder.append(this.version);
		builder.append(", content=byte[").append(content != null ? content.length : "null");
		builder.append("]");
		return builder.toString();
	}

}
