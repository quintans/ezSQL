package pt.quintans.ezSQL.orm.app.mappings.virtual;

import pt.quintans.ezSQL.orm.app.domain.IdentityDomain;

public class Book18 extends IdentityDomain<Long> {
    private String lang;
    private String name;

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Book18 [id=" + id + ", lang=" + lang + ", name=" + name + "]";
    }

}
