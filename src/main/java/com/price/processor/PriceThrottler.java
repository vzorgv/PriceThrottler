package com.price.processor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PriceThrottler implements PriceProcessor {

    private final static Logger logger = LogManager.getLogger(PriceThrottler.class);

    private final ConcurrentHashMap<PriceProcessor, CompletableFuture<Void>> runningTasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PriceProcessor, Subscription> subscriptions = new ConcurrentHashMap<>();

    @Override
    public void onPrice(String ccyPair, double rate) {

        for (var subscription: subscriptions.values()) {
            subscription.put(ccyPair, rate);
        }
    }

    @Override
    public void subscribe(PriceProcessor priceProcessor) {
        logger.error("Subscribed");
        var subscription = new Subscription();
        subscriptions.put(priceProcessor, subscription);
        runningTasks.put(priceProcessor, createTask(priceProcessor, subscription));
    }

    @Override
    public void  unsubscribe(PriceProcessor priceProcessor) {

        var subscription = subscriptions.get(priceProcessor);
        var task = runningTasks.get(priceProcessor);

        subscription.cancel();

        subscriptions.remove(priceProcessor);
        runningTasks.remove(priceProcessor);
    }

    private Runnable createJob(PriceProcessor processor, Subscription data) {
        return () -> {
            var running = true;
            while (running) {
                var price = data.take();

                logger.info("Data received");

                if (price == null) {
                    running = false;
                } else {
                    processor.onPrice(price.getCcyPair(), price.getRate());
                }
            }
        };
    }

    private CompletableFuture<Void> createTask(PriceProcessor processor, Subscription data) {

        var runnable = this.createJob(processor, data);
        var task = CompletableFuture.runAsync(runnable);
        return task;
    }
}
