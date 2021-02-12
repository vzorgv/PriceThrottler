package com.price.processor;

import java.util.HashSet;
import java.util.Set;


public class PriceThrottler implements PriceProcessor {

    private final Set<PriceProcessor> subscribers = new HashSet<>();

    @Override
    public void onPrice(String ccyPair, double rate) {

        for (var subscriber: subscribers) {
            subscriber.onPrice(ccyPair, rate);
        }
    }

    @Override
    public synchronized void subscribe(PriceProcessor priceProcessor) {
        subscribers.add(priceProcessor);
    }

    @Override
    public synchronized void  unsubscribe(PriceProcessor priceProcessor) {
        subscribers.remove(priceProcessor);
    }
}
