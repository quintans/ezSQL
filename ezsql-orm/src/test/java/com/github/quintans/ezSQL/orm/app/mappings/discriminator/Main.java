package com.github.quintans.ezSQL.orm.app.mappings.discriminator;


public class Main {
    private Long id;
    private String tipo;
    private Long fk;

    private Be be;
    private Ce ce;

    public Main() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

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

    public Be getBe() {
        return be;
    }

    public void setBe(Be be) {
        this.be = be;
    }

    public Ce getCe() {
        return ce;
    }

    public void setCe(Ce ce) {
        this.ce = ce;
    }

    @Override
    public String toString() {
        return "[id=" + id + ", tipo=" + tipo + ", fk=" + fk + ", be=" + be + ", ce=" + ce + "]";
    }

}
