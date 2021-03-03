package com.price.processor.throttler;

import java.util.concurrent.*;

import com.price.processor.PriceProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PriceThrottler implements PriceProcessor, AutoCloseable {

    private final static Logger logger = LogManager.getLogger(PriceThrottler.class);

    private final ConcurrentHashMap<PriceProcessor, CompletableFuture<Void>> tasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PriceProcessor, CurrencyPairPriceQueue> taskQueues = new ConcurrentHashMap<>();
    private final ExecutorService taskPool = Executors.newCachedThreadPool();

    @Override
    public void onPrice(String ccyPair, double rate) {

        for (var queue: taskQueues.values()) {
            queue.offer(new CurrencyPairPrice(ccyPair, rate));
        }
    }

    @Override
    public void subscribe(PriceProcessor priceProcessor) {
        var priceQueue = new CurrencyPairPriceQueue(getThrottlingStrategy());
        taskQueues.put(priceProcessor, priceQueue);
        tasks.put(priceProcessor, createTask(priceProcessor, priceQueue));
        logger.info(priceProcessor.toString() + " subscribed");
    }

    @Override
    public void  unsubscribe(PriceProcessor priceProcessor) {

        taskQueues.remove(priceProcessor);
        logger.info(priceProcessor.toString() + " unsubscribed");
    }

    @Override
    public void close() {

        for (var processor: tasks.keySet()) {
            unsubscribe(processor);
            tasks.remove(processor);
        }

        taskPool.shutdownNow();
    }

    private Runnable createJob(PriceProcessor processor, CurrencyPairPriceQueue queue) {
        return () -> {

            var isRunning = true;

            while(isRunning) {
                CurrencyPairPrice pairPrice;
                try {
                    pairPrice = queue.take();

                    if (pairPrice != null) {
                        processor.onPrice(pairPrice.getCcyPair(), pairPrice.getRate());
                        String infoMsg = String.format("Pair %s with price %f processed in processor %s", pairPrice.getCcyPair(), pairPrice.getRate(), processor);
                        logger.info(infoMsg);
                    }

                } catch (InterruptedException e) {
                    //logger.info("Task finalized");
                    isRunning = false;
                }
            }
        };
    }

    private CompletableFuture<Void> createTask(PriceProcessor processor, CurrencyPairPriceQueue data) {

        var runnable = this.createJob(processor, data);
        return CompletableFuture.runAsync(runnable, taskPool);
    }

    private ThrottlingStrategy getThrottlingStrategy() {
        return new DeliveryFreqRankThrottling();
    }
}
