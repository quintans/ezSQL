package com.github.quintans.ezSQL.repository;

import java.util.Collection;

public class PageResults<T> {
    // max number of records returned by the datasource
    private Long count;
    // if this is the last page
    private Boolean last;
    private Collection<T> results;

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Boolean getLast() {
        return last;
    }

    public void setLast(Boolean last) {
        this.last = last;
    }

    public Collection<T> getResults() {
        return results;
    }

    public void setResults(Collection<T> results) {
        this.results = results;
    }
}
