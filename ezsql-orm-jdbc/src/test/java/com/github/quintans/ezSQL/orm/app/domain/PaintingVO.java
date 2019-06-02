package com.github.quintans.ezSQL.orm.app.domain;

public class PaintingVO extends BaseDomainVO<Long> {
    private String name;
    private Double price;

    public PaintingVO(Long id, Integer version, String name, Double price) {
        super(id, version);
        this.name = name;
        this.price = price;
    }

    private PaintingVO(Builder builder) {
        super(builder.id, builder.version);
        name = builder.name;
        price = builder.price;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public Builder toBuilder() {
        Builder builder = new Builder();
        builder.id = getId();
        builder.version = getVersion();
        builder.name = getName();
        builder.price = getPrice();
        return builder;
    }

    public String getName() {
        return name;
    }

    public Double getPrice() {
        return price;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("PaintingVO [id=");
        builder.append(id);
        builder.append(", version=");
        builder.append(version);
        builder.append(", name=");
        builder.append(name);
        builder.append(", price=");
        builder.append(price);
        builder.append("]");
        return builder.toString();
    }


    public static final class Builder {
        private Long id;
        private Integer version;
        private String name;
        private Double price;

        private Builder() {
        }

        public Builder id(Long id) {
            this.id = id;
            return this;
        }

        public Builder version(Integer version) {
            this.version = version;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder price(Double price) {
            this.price = price;
            return this;
        }

        public PaintingVO build() {
            return new PaintingVO(this);
        }
    }
}
