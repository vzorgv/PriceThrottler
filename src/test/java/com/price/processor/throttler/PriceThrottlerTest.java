package com.price.processor.throttler;


import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class PriceThrottlerTest {

    private static final long THREAD_POOL_WARMUP_DELAY = 100;

    @Test
    @DisplayName("When price come then distributed to the listener")
    public void eventShouldBeDistributedToListener() {
        // Arrange
        var prices = new HashMap<String, Double>();

        prices.put("EURUSD", 6.28);

        var listener = SimplePriceProcessor.constructWithoutDelayInProcessing();
        var throttler = new PriceThrottler();

        throttler.subscribe(listener);

        // Act
        throttler.onPrice("EURUSD", 6.28);

        try {
            Thread.sleep(THREAD_POOL_WARMUP_DELAY);
            throttler.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Assert
        assertTrue(prices.equals(listener.getProcessedPrices()));
    }

    @Test
    @DisplayName("When several prices come then distributed to the all listeners")
    public void eventShouldBeDistributedToListeners() {
        // Arrange
        var prices = new HashMap<String, Double>();

        prices.put("EURUSD", 6.28);
        prices.put("USDRUB", 74.262);
        prices.put("EURRUB", 81.24);

        var firstListener = SimplePriceProcessor.constructWithoutDelayInProcessing();
        var secondListener = SimplePriceProcessor.constructWithoutDelayInProcessing();
        var throttler = new PriceThrottler();

        throttler.subscribe(firstListener);
        throttler.subscribe(secondListener);

        try {
            Thread.sleep(THREAD_POOL_WARMUP_DELAY);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Act
        var it = prices.entrySet().iterator();

        while (it.hasNext()) {
            var price = it.next();
            throttler.onPrice(price.getKey(), price.getValue());
        }

        try {
            Thread.sleep(THREAD_POOL_WARMUP_DELAY);
            throttler.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Assert
        assertTrue(prices.equals(firstListener.getProcessedPrices()), "First listener assertion");
        assertTrue(prices.equals(secondListener.getProcessedPrices()), "Second listener assertion");
    }


    @Test
    @DisplayName("ONLY LAST PRICE for each ccyPair matters for subscriber")
    public void theLastPriceShouldBeProcessed() {
        // Arrange
        var prices = new HashMap<String, Double>();

        prices.put("EURUSD", 8.28);

        var slowListener = new SimplePriceProcessor(10);
        var throttler = new PriceThrottler();

        throttler.subscribe(slowListener);

        try {
            Thread.sleep(THREAD_POOL_WARMUP_DELAY);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Act
        throttler.onPrice("EURUSD", 6.28);
        throttler.onPrice("EURUSD", 7.28);
        throttler.onPrice("EURUSD", 8.28);

        try {
            Thread.sleep(THREAD_POOL_WARMUP_DELAY);
            throttler.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Assert
        assertTrue(prices.equals(slowListener.getProcessedPrices()));
    }

    @Test
    @DisplayName("Not to miss rarely changing prices")
    public void theAllPricesShouldBeDelivered()
    {
        // Arrange
        var prices = new HashMap<String, Double>();

        prices.put("EURUSD", 8.28);
        prices.put("EURRUB", 11.0);

        var listener = SimplePriceProcessor.constructWithoutDelayInProcessing();
        var throttler = new PriceThrottler();

        throttler.subscribe(listener);

        try {
            Thread.sleep(THREAD_POOL_WARMUP_DELAY);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Act
        throttler.onPrice("EURUSD", 0.28);
        throttler.onPrice("EURUSD", 1.28);
        throttler.onPrice("EURUSD", 2.28);

        throttler.onPrice("EURRUB", 10.0);

        throttler.onPrice("EURUSD", 6.28);
        throttler.onPrice("EURUSD", 7.28);
        throttler.onPrice("EURUSD", 8.28);

        throttler.onPrice("EURRUB", 11.0);

        try {
            Thread.sleep(THREAD_POOL_WARMUP_DELAY);
            throttler.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Assert
        assertTrue(prices.equals(listener.getProcessedPrices()));
    }

    @Test
    @DisplayName("Slow subscribers should not impact fast subscribers")
    public void theTwoProcessorsShouldNotBlockEachOther(){

        // Arrange
        var prices = new HashMap<String, Double>();

        prices.put("EURUSD", 6.28);
        prices.put("USDRUB", 74.262);
        prices.put("EURRUB", 81.24);

        var fastListener = SimplePriceProcessor.constructWithoutDelayInProcessing();
        var slowListener = new SimplePriceProcessor(1000);
        var throttler = new PriceThrottler();

        throttler.subscribe(fastListener);
        throttler.subscribe(slowListener);

        try {
            Thread.sleep(THREAD_POOL_WARMUP_DELAY);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Act
        var it = prices.entrySet().iterator();

        while (it.hasNext()) {
            var price = it.next();
            throttler.onPrice(price.getKey(), price.getValue());
        }

        try {
            Thread.sleep(THREAD_POOL_WARMUP_DELAY);
            throttler.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Assert
        assertTrue(fastListener.getProcessedPrices().size() > slowListener.getProcessedPrices().size(), "Fast listener assertion");
    }
}