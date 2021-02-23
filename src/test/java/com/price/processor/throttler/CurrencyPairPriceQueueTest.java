package com.price.processor.throttler;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class CurrencyPairPriceQueueTest {

    @Test
    public void pairPriceShouldBeOfferedAndDeliveredWithSuccess() {
        // Arrange
        var queue = new CurrencyPairPriceQueue(new DeliveryFreqRankThrottling());
        var pricePair = new CurrencyPairPrice("AAA", 0.11);

        // Act
        var actual = queue.offer(pricePair);
        CurrencyPairPrice actualMessage = null;

        try {
            actualMessage = queue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Assert
        assertTrue(actual);
        assertEquals(pricePair, actualMessage);
    }

    @Test
    public void theRarelyChangedShouldBeDeliveredFirst() {
        // Arrange
        var queue = new CurrencyPairPriceQueue(new DeliveryFreqRankThrottling());

        final String frequentCcyPair = "EURUSD";
        final String rareCcyPair = "USDRUB";
        final String expected = "USDRUB";

        queue.offer(new CurrencyPairPrice(frequentCcyPair, 0.10));
        queue.offer(new CurrencyPairPrice(frequentCcyPair, 0.11));
        queue.offer(new CurrencyPairPrice(frequentCcyPair, 0.12));
        queue.offer(new CurrencyPairPrice(frequentCcyPair, 0.13));
        queue.offer(new CurrencyPairPrice(frequentCcyPair, 0.14));
        queue.offer(new CurrencyPairPrice(frequentCcyPair, 0.15));
        queue.offer(new CurrencyPairPrice(frequentCcyPair, 0.16));

        queue.offer(new CurrencyPairPrice(rareCcyPair, 0.78));

        queue.offer(new CurrencyPairPrice(frequentCcyPair, 0.17));
        queue.offer(new CurrencyPairPrice(frequentCcyPair, 0.18));

        CurrencyPairPrice actual = null;
        // Act
        try {
            actual = queue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Assert
        assertNotNull(actual);
        assertEquals(expected, actual.getCcyPair());
    }


    @Test
    public void onlyLastPriceShouldBeDelivered() {
        // Arrange
        var queue = new CurrencyPairPriceQueue(new DeliveryFreqRankThrottling());

        final String ccyPair = "EURUSD";
        final double expected = 0.18;

        queue.offer(new CurrencyPairPrice(ccyPair, 0.10));
        queue.offer(new CurrencyPairPrice(ccyPair, 0.11));
        queue.offer(new CurrencyPairPrice(ccyPair, 0.12));
        queue.offer(new CurrencyPairPrice(ccyPair, 0.13));
        queue.offer(new CurrencyPairPrice(ccyPair, 0.14));
        queue.offer(new CurrencyPairPrice(ccyPair, 0.15));
        queue.offer(new CurrencyPairPrice(ccyPair, 0.16));
        queue.offer(new CurrencyPairPrice(ccyPair, 0.17));
        queue.offer(new CurrencyPairPrice(ccyPair, 0.18));

        CurrencyPairPrice actual = null;
        // Act
        try {
            actual = queue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Assert
        assertNotNull(actual);
        assertEquals(expected, actual.getRate());
    }
}
