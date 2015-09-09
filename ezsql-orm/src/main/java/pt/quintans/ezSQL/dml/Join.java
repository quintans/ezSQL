package pt.quintans.ezSQL.dml;

import java.util.List;

import pt.quintans.ezSQL.db.Association;

public class Join {
	private List<PathElement> associations;
	private boolean fetch;

	public Join(List<PathElement> associations, boolean fetch) {
		this.fetch = fetch;
		this.associations = associations;
	}

	public List<PathElement> getPathElements() {
		return this.associations;
	}

	public Association[] getAssociations() {
        Association[] derived = new Association[associations.size()];
        int i = 0;
        for (PathElement pe : associations) {
            derived[i++] = pe.getDerived();
        }
		return derived;
	}

    public boolean isFetch() {
        return fetch;
    }

}
