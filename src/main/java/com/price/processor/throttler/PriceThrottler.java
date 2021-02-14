package com.price.processor.throttler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import com.price.processor.PriceProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PriceThrottler implements PriceProcessor, AutoCloseable {

    private final static Logger logger = LogManager.getLogger(PriceThrottler.class);

    private final ConcurrentHashMap<PriceProcessor, CompletableFuture<Void>> tasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PriceProcessor, TaskDataBuffer> taskBuffers = new ConcurrentHashMap<>();

    @Override
    public void onPrice(String ccyPair, double rate) {

        for (var subscription: taskBuffers.values()) {
            subscription.put(ccyPair, rate);
        }
    }

    @Override
    public void subscribe(PriceProcessor priceProcessor) {
        var subscription = new TaskDataBuffer();
        taskBuffers.put(priceProcessor, subscription);
        tasks.put(priceProcessor, createTask(priceProcessor, subscription));
        logger.trace(priceProcessor.toString() + " subscribed");
    }

    @Override
    public void  unsubscribe(PriceProcessor priceProcessor) {
        var subscription = taskBuffers.get(priceProcessor);
        if (subscription != null) {
            subscription.cancel();

            taskBuffers.remove(priceProcessor);
            logger.info(priceProcessor.toString() + " unsubscribed");
        }
    }

    @Override
    public void close() throws Exception {

        for (var processor: tasks.keySet()) {
            unsubscribe(processor);
            var task = tasks.get(processor);
            task.join();
        }
    }

    private Runnable createJob(PriceProcessor processor, TaskDataBuffer data) {
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

    private CompletableFuture<Void> createTask(PriceProcessor processor, TaskDataBuffer data) {

        var runnable = this.createJob(processor, data);
        var task = CompletableFuture.runAsync(runnable);
        return task;
    }
}
