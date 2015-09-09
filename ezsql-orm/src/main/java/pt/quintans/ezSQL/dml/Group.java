package pt.quintans.ezSQL.dml;

public class Group {
    private int position;
    private Function function;

    public Group(int position, Function function) {
        this.position = position;
        this.function = function;
    }

    public int getPosition() {
        return position;
    }

    public Function getFunction() {
        return function;
    }

}
