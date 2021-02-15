package com.price.processor.throttler;

import com.price.processor.PriceProcessor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 *  PriceProcessor implementation for testing purpose
 */
final class SimplePriceProcessor implements PriceProcessor {

    private final long sleepInMilli;

    private final ConcurrentHashMap<String, Double> processedPrices = new ConcurrentHashMap<>();

    /**
     * @param pauseInProcessingInMilliseconds defines the delay in milliseconds to process request
     */
    public SimplePriceProcessor(long pauseInProcessingInMilliseconds) {
        sleepInMilli = pauseInProcessingInMilliseconds;
    }

    /**
     * Constructs <c>SimplePriceProcessor</c> with zero delay
     * @return the <c>SimplePriceProcessor</c> instance
     */
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
