package com.price.processor.throttler;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Class implements a pipe between producer and consumer
 * If consumer is slower than producer it reduces a price pairs to deliver the only last value per price pair
 */
final class CurrencyPairPriceQueue {

    private final LinkedBlockingQueue<CurrencyPairPrice> pubSubQueue = new LinkedBlockingQueue<>();
    private final ThrottlingStrategy throttlingStrategy;


    public CurrencyPairPriceQueue(ThrottlingStrategy throttlingStrategy) {
        this.throttlingStrategy = throttlingStrategy;
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

        if (throttlingStrategy.isEmpty())
            fetchedItem = pubSubQueue.take(); // awaiting for the next item
        else
            fetchedItem = pubSubQueue.poll();

        if (fetchedItem != null) {
            throttlingStrategy.pushItem(fetchedItem);
            pubSubQueue.drainTo(pairPriceBatch);

            for (var item : pairPriceBatch) {
                throttlingStrategy.pushItem(item);
            }
        }

        fetchedItem = throttlingStrategy.popItem();

        return fetchedItem;
    }
}