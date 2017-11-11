package com.github.quintans.ezSQL.orm.app.mappings.discriminator;

import java.util.Set;

import com.github.quintans.ezSQL.orm.app.domain.IdentityDomain;

public class Thing extends IdentityDomain {
    private String dsc;

    private Set<Cena> cenas;

    public String getDsc() {
        return dsc;
    }

    public void setDsc(String dsc) {
        this.dsc = dsc;
    }

    public Set<Cena> getCenas() {
        return cenas;
    }

    public void setCenas(Set<Cena> cenas) {
        this.cenas = cenas;
    }

    @Override
    public String toString() {
        return "[id=" + id + ", dsc=" + dsc + ", cenas=" + cenas + "]";
    }

}
