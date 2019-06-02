package com.github.quintans.ezSQL.orm.app.mappings.discriminator;

import com.github.quintans.ezSQL.orm.app.domain.IdentityDomain;


public class Cena extends IdentityDomain {
    private String tipo;
    private Long fk;

    public String getTipo() {
        return tipo;
    }

    public void setTipo(String tipo) {
        this.tipo = tipo;
    }

    public Long getFk() {
        return fk;
    }

    public void setFk(Long fk) {
        this.fk = fk;
    }

    @Override
    public String toString() {
        return "[id=" + id + ", tipo=" + tipo + ", fk=" + fk + "]";
    }

}
