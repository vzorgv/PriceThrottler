package com.price.processor.throttler;

import com.price.processor.PriceProcessor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class SimplePriceProcessor implements PriceProcessor {

    private final long sleepInMilli;

    private final ConcurrentHashMap<String, Double> processedPrices = new ConcurrentHashMap<>();

    public SimplePriceProcessor(long pauseInProcessingInMilliseconds) {
        sleepInMilli = pauseInProcessingInMilliseconds;
    }

    public static SimplePriceProcessor constructWithoutDelayInProcessing() {
        return new SimplePriceProcessor(0);
    }

    @Override
    public void onPrice(String ccyPair, double rate) {
        if (sleepInMilli != 0) {
            try {
                Thread.sleep(sleepInMilli);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        processedPrices.put(ccyPair, rate);
    }

    @Override
    public void subscribe(PriceProcessor priceProcessor) {
        //Left it empty on purpose
    }

    @Override
    public void unsubscribe(PriceProcessor priceProcessor) {
        //Left it empty on purpose
    }

    public Map<String, Double> getProcessedPrices() {
        return processedPrices;
    }
}
