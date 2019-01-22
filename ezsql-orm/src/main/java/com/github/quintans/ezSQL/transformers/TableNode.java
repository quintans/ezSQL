package com.github.quintans.ezSQL.transformers;

import java.util.*;

public class TableNode {
    public interface Instantiate {
        Object apply(Object parentInstance, String name);
    }

    /**
     * table alias
     */
    private String tableAlias;
    /**
     * association alias
     */
    private String associationAlias;
    private TableNode parent;
    /**
     * domain object. This will never be a collection.
     */
    private Object instance;
    private List<ColumnNode> columnNodes = new ArrayList<>();
    private List<TableNode> tableNodes = new ArrayList<>();
    /**
     * domain instances for each association
     */
    private Map<String, Object> childInstances = new HashMap<>();

    public TableNode(String tableAlias, String associationAlias) {
        this.tableAlias = tableAlias;
        this.associationAlias = associationAlias;
    }

    public String getAssociationAlias() {
        return associationAlias;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public void setParent(TableNode parent) {
        this.parent = parent;
    }

    public Object getInstance() {
        return instance;
    }

    public void setInstance(Object instance) {
        this.instance = instance;
    }

    public void addColumnNode(ColumnNode columnNode) {
        if (!columnNodes.contains(columnNode)) {
            this.columnNodes.add(columnNode);
        }
    }

    public void reset() {
        instance = null;
        childInstances.clear();
        for (TableNode tableNode : tableNodes) {
            tableNode.reset();
        }
    }

    public Object getFieldInstanceIfAbsent(String name, Instantiate instantiate) {
        instance = getInstanceIfAbsent(instantiate);
        return childInstances.computeIfAbsent(name, key -> instantiate.apply(instance, key));
    }

    public Object getInstanceIfAbsent(Instantiate instantiate) {
        if (instance == null) {
            if (parent == null) {
                instance = instantiate.apply(null, associationAlias);
            } else {
                instance = parent.getFieldInstanceIfAbsent(associationAlias, instantiate);
            }
        }
        return instance;
    }

    public List<ColumnNode> getColumnNodes() {
        return columnNodes;
    }

    public void addTableNode(TableNode tableNode) {
        if (!tableNodes.contains(tableNode)) {
            tableNode.setParent(this);
            tableNodes.add(tableNode);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableNode tableNode = (TableNode) o;
        return associationAlias.equals(tableNode.associationAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(associationAlias);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(tableAlias + " {\n");
        for (ColumnNode cn : columnNodes) {
            sb.append("  ").append(cn).append("\n");
        }
        sb.append("} " + associationAlias);
        return sb.toString();
    }
}
