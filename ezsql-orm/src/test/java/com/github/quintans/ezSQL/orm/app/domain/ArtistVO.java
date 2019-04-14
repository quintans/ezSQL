package com.github.quintans.ezSQL.orm.app.domain;

import java.util.Set;

public class ArtistVO extends BaseDomainVO<Long> {
    private String name;
    private Set<PaintingVO> paintings;

    public ArtistVO(Long id, Integer version, String name, Set<PaintingVO> paintings) {
        super(id, version);
        this.name = name;
        this.paintings = paintings;
    }

    private ArtistVO(Builder builder) {
        super(builder.id, builder.version);
        name = builder.name;
        paintings = builder.paintings;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public Builder toBuilder() {
        Builder builder = new Builder();
        builder.id = getId();
        builder.version = getVersion();
        builder.name = getName();
        builder.paintings = getPaintings();
        return builder;
    }

    public String getName() {
        return this.name;
    }

    public Set<PaintingVO> getPaintings() {
        return this.paintings;
    }

    @Override
    public String toString() {
        final int maxLen = 10;
        StringBuilder builder = new StringBuilder();
        builder.append("ArtistVO [id=").append(this.id);
        builder.append(", version=").append(this.version);
        builder.append(", name=").append(this.name);
        builder.append(", paintings=");
        builder.append(this.paintings != null ? toString(this.paintings, maxLen) : null);
        builder.append("]");
        return builder.toString();
    }

    public static final class Builder {
        private Long id;
        private Integer version;
        private String name;
        private Set<PaintingVO> paintings;

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

        public Builder paintings(Set<PaintingVO> paintings) {
            this.paintings = paintings;
            return this;
        }

        public ArtistVO build() {
            return new ArtistVO(this);
        }
    }
}
