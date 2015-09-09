package pt.quintans.ezSQL.db;

import java.util.ArrayList;

/**
 * Class used to force the declaration of the alias when creating an association
 * 
 * @author paulo.quintans
 *
 */
public class Relashionships extends ArrayList<Relation> {
    private static final long serialVersionUID = 1L;

    public Relashionships(int size) {
        super(size);
    }

    public Association AS(String alias) {
        return new Association(this.toArray(new Relation[this.size()])).AS(alias);
    }

}
