package com.github.quintans.ezSQL.driver;


public interface QueryBuilder {
    String getWherePart();
    String getColumnPart();
    String getFromPart();
    String getGroupPart();
    String getHavingPart();
    String getOrderPart();
    String getUnionPart();
    
    String getJoinPart();    
}
