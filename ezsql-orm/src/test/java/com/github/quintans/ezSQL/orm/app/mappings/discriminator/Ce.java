package com.github.quintans.ezSQL.orm.app.mappings.discriminator;

import java.util.Set;

public class Ce {
    private Long id;
    private String dsc;

    private Set<Main> mains;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDsc() {
        return dsc;
    }

    public void setDsc(String dsc) {
        this.dsc = dsc;
    }

    public Set<Main> getMains() {
        return mains;
    }

    public void setMains(Set<Main> mains) {
        this.mains = mains;
    }

    @Override
    public String toString() {
        return "[id=" + id + ", dsc=" + dsc + ", mains=" + mains + "]";
    }

}
