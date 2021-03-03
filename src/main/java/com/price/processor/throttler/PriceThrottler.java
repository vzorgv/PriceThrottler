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

        for (var entry: taskQueues.entrySet()) {
            var queue = entry.getValue();
            var processor = entry.getKey();
            queue.offer(new CurrencyPairPrice(ccyPair, rate));
            scheduleTask(processor);
        }
    }

    @Override
    public void subscribe(PriceProcessor priceProcessor) {
        var priceQueue = new CurrencyPairPriceQueue(getThrottlingStrategy());
        taskQueues.put(priceProcessor, priceQueue);
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

        taskPool.shutdown();
    }

    private void scheduleTask(PriceProcessor processor) {

        var task = tasks.get(processor);

        if (task == null || task.isDone()) {
            var queue = taskQueues.get(processor);
            var runnableTask = createTask(processor, queue);
            task = CompletableFuture.runAsync(runnableTask, taskPool);
        }

        tasks.put(processor, task);
    }

    private Runnable createTask(PriceProcessor processor, CurrencyPairPriceQueue queue) {
        return () -> {

            var isRunning = true;

            do {
                    CurrencyPairPrice pairPrice;
                    try {
                            pairPrice = queue.poll();

                            if (pairPrice == null) {
                                isRunning = false;
                            } else {
                                processor.onPrice(pairPrice.getCcyPair(), pairPrice.getRate());
                            }
                    } catch (InterruptedException e) {
                        logger.info("Task interrupted");
                        isRunning = false;
                    }
            } while(isRunning);
        };
    }

    private ThrottlingStrategy getThrottlingStrategy() {
        return new DeliveryFreqRankThrottling();
    }
}
