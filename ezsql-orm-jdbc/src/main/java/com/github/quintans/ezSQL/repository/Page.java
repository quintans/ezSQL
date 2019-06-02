package com.github.quintans.ezSQL.repository;

public class Page {
    private Boolean countRecords;
    private Long page;
    private Long pageSize;
    private String orderBy;
    private Boolean ascending;

    public Boolean getCountRecords() {
        return countRecords;
    }

    public void setCountRecords(Boolean countRecords) {
        this.countRecords = countRecords;
    }

    public Long getPage() {
        return page;
    }

    public void setPage(Long page) {
        this.page = page;
    }

    public Long getPageSize() {
        return pageSize;
    }

    public void setPageSize(Long pageSize) {
        this.pageSize = pageSize;
    }

    public String getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(String orderBy) {
        this.orderBy = orderBy;
    }

    public Boolean getAscending() {
        return ascending;
    }

    public void setAscending(Boolean ascending) {
        this.ascending = ascending;
    }
}
