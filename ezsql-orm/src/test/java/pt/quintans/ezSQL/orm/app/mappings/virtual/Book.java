package pt.quintans.ezSQL.orm.app.mappings.virtual;

import pt.quintans.ezSQL.orm.app.domain.BaseDomain;

public class Book extends BaseDomain<Long> {
    private Author author;
    private Double price;
    private Book18 i18n;

    public Author getAuthor() {
        return this.author;
    }

    public void setAuthor(Author author) {
        this.author = author;
    }

    public Double getPrice() {
        return this.price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Book18 getI18n() {
        return i18n;
    }

    public void setI18n(Book18 i18n) {
        this.i18n = i18n;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Book [id=");
        builder.append(this.id);
        builder.append(", version=");
        builder.append(this.version);
        builder.append(", i18n=");
        builder.append(this.i18n);
        builder.append(", author=");
        builder.append(this.author);
        builder.append(", price=");
        builder.append(this.price);
        builder.append("]");
        return builder.toString();
    }

}
