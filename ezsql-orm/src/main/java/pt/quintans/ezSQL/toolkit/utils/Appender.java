package pt.quintans.ezSQL.toolkit.utils;

public class Appender {
    private StringBuilder sb = new StringBuilder();
    private String separator;
    
    public Appender() {
    }
    
    public Appender(String separator) {
        this.separator = separator;
    }
    
    public Appender add(Object... a) {
        for(Object o : a){
            if(sb.length() > 0){
                sb.append(separator);
            }
            sb.append(o);
        }
        return this;
    }

    public Appender append(Object... a) {
        for(Object o : a){
            sb.append(o);
        }
        return this;
    }

    public Appender addAsOne(Object... a) {
        if(sb.length() > 0){
            sb.append(separator);
        }
        for(Object o : a){
            sb.append(o);
        }
        return this;
    }

    @Override
    public String toString() {
        return sb.toString();
    }
    
    
}
