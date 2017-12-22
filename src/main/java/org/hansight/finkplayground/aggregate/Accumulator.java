package org.hansight.finkplayground.aggregate;

public class Accumulator {
    public long time;
    public String bid;
    public double sum;
    public int count;

    public Accumulator(long time, String bid, double sum, int count) {
        this.time = time;
        this.bid = bid;
        this.sum = sum;
        this.count = count;
    }
}