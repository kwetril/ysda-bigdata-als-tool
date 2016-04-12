package com.ysda.bigdata;

/**
 * Created by xakl on 12.04.2016.
 */
public class StopWatch {
    private long startTime = 0;
    private long stopTime = 0;
    private boolean running = false;

    public void start() {
        startTime = System.currentTimeMillis();
        running = true;
    }

    //returns elaspsed time in milliseconds
    public long stop() {
        stopTime = System.currentTimeMillis();
        running = false;
        return  getElapsedTime();
    }

    //returns elaspsed time in milliseconds
    public long getElapsedTime() {
        long elapsed;
        if (running) {
            elapsed = (System.currentTimeMillis() - startTime);
        } else {
            elapsed = (stopTime - startTime);
        }
        return elapsed;
    }

    //returns elaspsed time in seconds
    public long getElapsedTimeSecs() {
        return getElapsedTime() / 1000;
    }
}
