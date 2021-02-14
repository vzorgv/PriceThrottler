package com.price.processor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class PriceThrottler implements PriceProcessor {

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
        var data = new Subscription();
        subscriptions.put(priceProcessor, data);
        runningTasks.put(priceProcessor, createTask(priceProcessor, data));
    }

    @Override
    public void  unsubscribe(PriceProcessor priceProcessor) {

        var subscription = subscriptions.get(priceProcessor);
        var task = runningTasks.get(priceProcessor);

        subscription.cancel();
        subscriptions.remove(priceProcessor);

        task.join();
        runningTasks.remove(priceProcessor);
    }

    private Runnable createRunnable(PriceProcessor processor, Subscription data) {
        return () -> {
            var running = true;
            while (running) {
                var price = data.take();

                if (price == null) {
                    running = false;
                } else {
                    processor.onPrice(price.getCcyPair(), price.getRate());
                }
            }
        };
    }

    private CompletableFuture<Void> createTask(PriceProcessor processor, Subscription data) {

        var runnable = this.createRunnable(processor, data);
        var task = CompletableFuture.runAsync(runnable);
        return task;
    }
}
