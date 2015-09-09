package pt.quintans.ezSQL.driver;


public interface UpdateBuilder {
    String getColumnPart();
    String getTablePart();
    String getWherePart();
}
