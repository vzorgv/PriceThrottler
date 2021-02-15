package com.price.processor.throttler;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Class implements a pipe between producer and consumer
 * If consumer is slower than producer it reduces a price pairs to deliver the only last value per price pair
 */
final class CurrencyPairPriceQueue {

    // Use map since Set doesn't allow to get contents by key
    private final HashMap<CountedPairPrice, CountedPairPrice> reducedPairPrices = new HashMap<>();
    private final LinkedBlockingQueue<CurrencyPairPrice> pubSubQueue = new LinkedBlockingQueue<>();

    /**
     * Class implements the item to count frequency of pair prices
     */
    private static class CountedPairPrice {

        private final CurrencyPairPrice currencyPairPrice;
        private Integer count;

        public CountedPairPrice(CurrencyPairPrice currencyPairPrice) {
            this.currencyPairPrice = currencyPairPrice;
            count = 0;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) { this.count = count; }

        public CurrencyPairPrice getCurrencyPairPrice() {
            return currencyPairPrice;
        }

        @Override
        public boolean equals(Object o) {

            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CountedPairPrice that = (CountedPairPrice) o;

            return currencyPairPrice.getCcyPair().equals(that.currencyPairPrice.getCcyPair());
        }

        @Override
        public int hashCode() {
            return currencyPairPrice.getCcyPair().hashCode();
        }
    }

    /**
     * Implements non blocking write operation
     * @param pairPrice pair of currency
     * @return True if inserted successfully
     */
    public boolean offer(CurrencyPairPrice pairPrice) {
        return pubSubQueue.offer(pairPrice);
    }

    /**
     * Implements blocking read.
     * @return <c>CurrencyPairPrice</c> from producer
     * @throws InterruptedException the Interrupted exception
     */
    public CurrencyPairPrice take() throws InterruptedException {
        LinkedList<CurrencyPairPrice> pairPriceBatch = new LinkedList<>();

        CurrencyPairPrice fetchedItem;

        if (reducedPairPrices.isEmpty()) {
            fetchedItem = pubSubQueue.take(); // awaiting for the next item
        } else {
            fetchedItem = pubSubQueue.poll();
        }

        if (fetchedItem != null) {
            addPairPriceToBuffer(fetchedItem);
            pubSubQueue.drainTo(pairPriceBatch);

            for (var item : pairPriceBatch) {
                addPairPriceToBuffer(item);
            }
        }

        var pairPrice = getRarePairPrice();

        if (pairPrice != null)
            removePairPriceFromBuffer(pairPrice);

        return pairPrice;
    }

    private void addPairPriceToBuffer(CurrencyPairPrice currencyPairPrice) {

        var newCountedPair = new CountedPairPrice(currencyPairPrice);
        var existingElement = reducedPairPrices.get(newCountedPair);

        if (existingElement != null) {
            newCountedPair.setCount(existingElement.getCount() + 1);
        }

        reducedPairPrices.put(newCountedPair, newCountedPair);
    }

    private void removePairPriceFromBuffer(CurrencyPairPrice currencyPairPrice) {
        reducedPairPrices.remove(new CountedPairPrice(currencyPairPrice));
    }

    private CurrencyPairPrice getRarePairPrice() {
        var ret = reducedPairPrices
                .values()
                .stream()
                .min(Comparator.comparing(CountedPairPrice::getCount))
                .orElse(null);

        return ret != null ? ret.getCurrencyPairPrice() : null;
    }
}