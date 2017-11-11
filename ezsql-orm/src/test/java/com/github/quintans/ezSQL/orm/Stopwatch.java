package com.github.quintans.ezSQL.orm;

public class Stopwatch {
    private long start;
    private long counter;
    private int iteractions;
    
    public static Stopwatch createAndStart() {
        Stopwatch sw = new Stopwatch();
        sw.start();
        return sw;
    }
    
    public Stopwatch reset(){
        counter = 0;
        iteractions = 0;
        return this;
    }
    
    public void start(){
        start = System.nanoTime();
    }
    
    public Stopwatch stop() {
        iteractions++;
        counter += System.nanoTime() - start;
        return this;
    }
    
    public Stopwatch showTotal(String msg) {
        System.out.println("===>   TOTAL:" + (msg != null ? " ["+msg+"]" : "") + " time: " + counter/1e6 + " ms");
        return this;
    }
    
    public Stopwatch showAverage(String msg) {
        showAverage(msg, 0);
        return this;
    }
    
    public Stopwatch showAverage(String msg, int number) {
        if(number == 0)
            number = iteractions;
        
        System.out.println("===> AVERAGE:" + (msg != null ? " ["+msg+"]" : "") + " time: " + counter/number/1e6 + " ms");
        return this;
    }
}
