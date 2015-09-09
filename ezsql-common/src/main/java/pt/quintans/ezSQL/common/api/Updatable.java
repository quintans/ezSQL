package pt.quintans.ezSQL.common.api;

import java.util.Set;

public interface Updatable {
    /**
     * Retrive property names that were changed
     * 
     * @return names of the changed properties 
     */
    Set<String> changed();
    /**
     * clear changes
     */
    void clear();
}
