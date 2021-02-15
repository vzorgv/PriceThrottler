package com.price.processor.throttler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import com.price.processor.PriceProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PriceThrottler implements PriceProcessor, AutoCloseable {

    private final static Logger logger = LogManager.getLogger(PriceThrottler.class);

    private final ConcurrentHashMap<PriceProcessor, CompletableFuture<Void>> tasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PriceProcessor, CurrencyPriceQueue> taskQueues = new ConcurrentHashMap<>();

    @Override
    public void onPrice(String ccyPair, double rate) {

        for (var subscription: taskQueues.values()) {
            subscription.offer(new TaskData(ccyPair, rate));
        }
    }

    @Override
    public void subscribe(PriceProcessor priceProcessor) {
        var priceQueue = new CurrencyPriceQueue();
        taskQueues.put(priceProcessor, priceQueue);
        tasks.put(priceProcessor, createTask(priceProcessor, priceQueue));
    }

    @Override
    public void  unsubscribe(PriceProcessor priceProcessor) {
        var queue = taskQueues.get(priceProcessor);

        if (queue != null) {
            taskQueues.remove(priceProcessor);
            try {
                queue.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            logger.info(priceProcessor.toString() + " unsubscribed");
        }
    }

    @Override
    public void close() throws Exception {

        for (var processor: tasks.keySet()) {
            unsubscribe(processor);

            var task = tasks.get(processor);
            tasks.remove(processor);
            task.join();
        }
    }

    private Runnable createJob(PriceProcessor processor, CurrencyPriceQueue queue) {
        return () -> {
            var running = true;
            do {
                logger.info("Job for " + processor + " awaiting");
                var event = queue.take();

                if (CurrencyPriceQueue.TERMINATION_EVENT_ID.equals(event.getCcyPair())) {
                    running = false;
                    logger.info(String.format("Job for processor %s finalized", processor.toString()));
                } else {
                    processor.onPrice(event.getCcyPair(), event.getRate());
                    String infoMsg = String.format("Pair %s with price %f processed in processor %s", event.getCcyPair(), event.getRate(), processor.toString());
                    logger.info(infoMsg);
                }
            } while(running);
        };
    }


    //TODO: obsolete
    private Runnable DEL_createJob(PriceProcessor processor, CurrencyPriceQueue data) {
        return () -> {
            var running = true;
            while (running) {
                logger.info("Job for " + processor + " awaiting");
                var price = data.take();

                if (price == null) {
                    logger.info("Job for " + processor + " exiting");
                    running = false;
                } else {
                    processor.onPrice(price.getCcyPair(), price.getRate());
                    String infoMsg = String.format("Pair %s with price %f processed in processor %s", price.getCcyPair(), price.getRate(), processor.toString());
                    logger.info(infoMsg);
                }
            }
        };
    }

    private CompletableFuture<Void> createTask(PriceProcessor processor, CurrencyPriceQueue data) {

        var runnable = this.createJob(processor, data);
        return CompletableFuture.runAsync(runnable);
    }
}
