package pt.quintans.ezSQL.orm.app.dtos;

import java.util.Set;

import pt.quintans.ezSQL.orm.app.domain.Painting;

public class ArtistValueDTO extends BaseDTO<Long> {
    private String name;
    private Set<Painting> paintings;
    private Double value;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }
    
    public Set<Painting> getPaintings() {
        return paintings;
    }

    public void setPaintings(Set<Painting> paintings) {
        this.paintings = paintings;
    }

    @Override
    public String toString() {
        return "ArtistValueDTO {id=" + id + ", name=" + name + ", value=" + value + ", paintings=" + paintings + "}";
    }

}
