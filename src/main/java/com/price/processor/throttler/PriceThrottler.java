package com.price.processor.throttler;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import com.price.processor.PriceProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PriceThrottler implements PriceProcessor, AutoCloseable {

    private final static Logger logger = LogManager.getLogger(PriceThrottler.class);
    public final static String TERMINATION_MESSAGE_ID = UUID.randomUUID().toString();

    private final ConcurrentHashMap<PriceProcessor, CompletableFuture<Void>> tasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PriceProcessor, CurrencyPairPriceQueue> taskQueues = new ConcurrentHashMap<>();

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
        var queue = taskQueues.get(priceProcessor);

        taskQueues.remove(priceProcessor);

        var terminationMsg = createTerminationMessage();
        queue.offer(terminationMsg);

        logger.info(priceProcessor.toString() + " unsubscribed");

    }

    @Override
    public void close() {

        for (var processor: tasks.keySet()) {
            unsubscribe(processor);

            var task = tasks.get(processor);
            tasks.remove(processor);
            task.join();
        }
    }

    private static CurrencyPairPrice createTerminationMessage() {
        return new CurrencyPairPrice(TERMINATION_MESSAGE_ID, 0.0);
    }

    private static boolean isTerminationMsg(CurrencyPairPrice pairPrice) {
        return pairPrice != null && pairPrice.getCcyPair().equals(TERMINATION_MESSAGE_ID);
    }

    private Runnable createJob(PriceProcessor processor, CurrencyPairPriceQueue queue) {
        return () -> {
            var running = true;
            do {
                CurrencyPairPrice pairPrice = null;
                try {
                    pairPrice = queue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if (pairPrice != null) {
                    if (isTerminationMsg(pairPrice)) {
                        running = false;
                        logger.info(String.format("Job for processor %s finalized", processor));
                    } else {
                        processor.onPrice(pairPrice.getCcyPair(), pairPrice.getRate());
                        String infoMsg = String.format("Pair %s with price %f processed in processor %s", pairPrice.getCcyPair(), pairPrice.getRate(), processor);
                        logger.info(infoMsg);
                    }
                }
            } while(running);
        };
    }

    private CompletableFuture<Void> createTask(PriceProcessor processor, CurrencyPairPriceQueue data) {

        var runnable = this.createJob(processor, data);
        return CompletableFuture.runAsync(runnable);
    }

    private ThrottlingStrategy getThrottlingStrategy() {
        return new DeliveryFreqRankThrottling();
    }
}
