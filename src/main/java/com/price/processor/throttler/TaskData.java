package com.price.processor.throttler;

class TaskData {

    private final String ccyPair;
    private double rate;
    private boolean isProcessed;

    public TaskData(String ccyPair, double rate) {
        this.ccyPair = ccyPair;
        this.rate = rate;
        this.isProcessed = false;
    }

    public String getCcyPair() {
        return ccyPair;
    }

    public double getRate() {
        return rate;
    }

    public void setRate(double rate) {
        this.rate = rate;
    }

    public boolean isProcessed() {
        return isProcessed;
    }

    public void setProcessed(boolean processed) {
        isProcessed = processed;
    }
}
