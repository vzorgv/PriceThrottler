package com.price.processor.throttler;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;


//@Timeout(value = 5)
public class PriceThrottlerTest {

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

        while (listener.getProcessedPrices().isEmpty());

        double actual = listener.getProcessedPrices().get("EURUSD");
        throttler.close();

        // Assert
        assertEquals(6.28, actual);
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

        // Act

        for (var price : prices.entrySet()) {
            throttler.onPrice(price.getKey(), price.getValue());
        }

        while (firstListener.getProcessedPrices().size() < 3
                || secondListener.getProcessedPrices().size() < 3) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        throttler.close();

        // Assert
        assertEquals(prices, firstListener.getProcessedPrices(), "First listener assertion");
        assertEquals(prices, secondListener.getProcessedPrices(), "Second listener assertion");
    }


    @Test
    @DisplayName("ONLY LAST PRICE for each ccyPair matters for subscriber")
    public void theLastPriceShouldBeProcessed() {
        // Arrange
        var prices = new HashMap<String, Double>();

        prices.put("EURUSD", 8.28);

        var slowListener = new SimplePriceProcessor(5);
        var throttler = new PriceThrottler();

        throttler.subscribe(slowListener);

        // Act
        throttler.onPrice("EURUSD", 6.28);
        throttler.onPrice("EURUSD", 7.28);
        throttler.onPrice("EURUSD", 8.28);

        while (slowListener.getProcessedPrices().isEmpty());

        throttler.close();

        double actual = slowListener.getProcessedPrices().get("EURUSD");

        // Assert
        assertEquals(8.28d, actual);
    }

    @Test
    @DisplayName("Not to miss rarely changing prices")
    public void theAllPricesShouldBeDelivered()
    {
        // Arrange
        var prices = new HashMap<String, Double>();

        prices.put("EURUSD", 8.28);
        prices.put("EURRUB", 11.0);

        var listener = new SimplePriceProcessor(10);
        var throttler = new PriceThrottler();

        throttler.subscribe(listener);

        // Act
        throttler.onPrice("EURUSD", 0.28);
        throttler.onPrice("EURUSD", 1.28);
        throttler.onPrice("EURUSD", 2.28);

        throttler.onPrice("EURRUB", 10.0);

        throttler.onPrice("EURUSD", 6.28);
        throttler.onPrice("EURUSD", 7.28);
        throttler.onPrice("EURUSD", 8.28);

        throttler.onPrice("EURRUB", 11.0);

        while (listener.getProcessedPrices().size() < 2) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Assert
        assertEquals(prices, listener.getProcessedPrices());
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

        // Act

        for (var price : prices.entrySet()) {
            throttler.onPrice(price.getKey(), price.getValue());
        }

        while (fastListener.getProcessedPrices().size() < 3);

        throttler.close();

        // Assert
        assertTrue(fastListener.getProcessedPrices().size() > slowListener.getProcessedPrices().size(), "Fast listener assertion");
    }

    @Test
    public void whenManySubscribersRanThrottlerDoesntHang() {

        PriceThrottler throttler = new PriceThrottler();

        for (int i = 0; i < 200; i++) {
            var listener = SimplePriceProcessor.constructWithoutDelayInProcessing();
            throttler.subscribe(listener);
        }

        for (int i = 0 ; i < 1_000; i++) {

            throttler.onPrice("EURUSD", i);
            throttler.onPrice("EURUSD", i + 1);
            throttler.onPrice("EURUSD", i + 2);

            throttler.onPrice("EURRUB", i + 3);

            throttler.onPrice("EURUSD", i + 4);
            throttler.onPrice("EURUSD", i + 5);
            throttler.onPrice("EURUSD", i + 6);

            throttler.onPrice("EURRUB", i + 7);
        }

        throttler.close();
    }
}