package com.github.quintans.ezSQL.orm.app.mappings.virtual;

import java.util.Set;

import com.github.quintans.ezSQL.orm.app.domain.BaseDomain;

public class Author extends BaseDomain<Long> {
	private String		name;
	private Set<Book>	books;

	public String getName() {
		return this.name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Set<Book> getBooks() {
		return this.books;
	}

	public void setBooks(Set<Book> books) {
		this.books = books;
	}

	@Override
	public String toString() {
		final int maxLen = 10;
		StringBuilder builder = new StringBuilder();
		builder.append("Author [id=");
		builder.append(this.id);
		builder.append(", version=");
		builder.append(this.version);
		builder.append(", name=");
		builder.append(this.name);
		builder.append(", books=");
		builder.append(this.books != null ? toString(this.books, maxLen) : null);
		builder.append("]");
		return builder.toString();
	}

}
