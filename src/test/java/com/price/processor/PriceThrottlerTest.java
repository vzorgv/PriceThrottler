package com.price.processor;


import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;


public class PriceThrottlerTest {

    @Test
    public void eventShouldBeDistributedToListener() {
        // Arrange
        var prices = new HashMap<String, Double>();

        prices.put("EURUSD", 6.28);

        var firstListener = SimplePriceProcessor.constructWithoutDelayInProcessing();
        var throttler = new PriceThrottler();

        throttler.subscribe(firstListener);

        // Act
        throttler.onPrice("EURUSD", 6.28);
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        throttler.unsubscribe(firstListener);

        // Assert
        assertTrue(prices.equals(firstListener.getProcessedPrices()));
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
        //throttler.subscribe(secondListener);

        // Act
        var it = prices.entrySet().iterator();

        while (it.hasNext()) {
            var price = it.next();
            throttler.onPrice(price.getKey(), price.getValue());
        }

        //throttler.unsubscribe(firstListener);
        //throttler.unsubscribe(secondListener);

        // Assert
        assertTrue(prices.equals(firstListener.getProcessedPrices()));
    }


    @Test
    @DisplayName("ONLY LAST PRICE for each ccyPair matters for subscribers")
    public void theLastPriceShouldBeProcessed() {

    }

    @Test
    @DisplayName("Slow subscribers should not impact fast subscribers")
    public void theTwoProccessorsShouldNotBlockEachOther() {

    }

    @Test
    @DisplayName("Not to miss rarely changing prices")
    public void theAllPricesShouldBeDelivered() {

    }

    @Test
    @DisplayName("Unsubscribe for running processor should be executed successfully")
    public void theRunningSubscriberShouldBeUnsubscribeSuccessfully() {

    }

    @Test
    @DisplayName("Unsubscribe for idle processor")
    public void theIdleSubscriberShouldBeUnsubscribeSuccessfully() {

    }


    @Test
    @DisplayName("Unsubscribe for not existed processor")
    public void theNotSubscribedProcessorShouldNotThrowException() {

    }
}