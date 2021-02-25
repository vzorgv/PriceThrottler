package com.price.processor.throttler;

/**
 * Implements an immutable item to interchange between producer and consumer
 */
final class CurrencyPairPrice {

    private final String ccyPair;
    private final double rate;

    public CurrencyPairPrice(String ccyPair, double rate) {
        this.ccyPair = ccyPair;
        this.rate = rate;
    }

    public String getCcyPair() {
        return ccyPair;
    }
    public double getRate() {
        return rate;
    }

    @Override
    public String toString() {
        return String.format("Pair price {ccyPair %s rate= %f}", ccyPair, rate);
    }
}
