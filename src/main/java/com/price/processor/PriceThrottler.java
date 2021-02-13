package com.price.processor;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;


public class PriceThrottler implements PriceProcessor {

    private final Set<PriceProcessor> subscribers = new HashSet<>();

    // Use map instead of queue since only one last element in buffer required for price processor
    private final ConcurrentHashMap<PriceProcessor, TaskArguments> subscriberData = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PriceProcessor, CompletableFuture<Void>> runningTasks = new ConcurrentHashMap<>();

    private class TaskArguments {

        private final String ccyPair;
        private  final double rate;

        private boolean isProcessed;

        public TaskArguments(String ccyPair, double rate) {
            this.ccyPair = ccyPair;
            this.rate = rate;
            this.isProcessed = false;
        }

        public String getCcyPair() {
            return ccyPair;
        }

        public double getRate() {
            return rate;
        }

        public boolean isProcessed() { return isProcessed; }

        public void setProcessed() { isProcessed = true; }
    }


    @Override
    public void onPrice(String ccyPair, double rate) {

        for (var subscriber: subscribers) {

            var taskArgs = new TaskArguments(ccyPair, rate);
            this.setDataToProcess(subscriber, taskArgs);
            this.scheduleTask(subscriber);
        }
    }

    @Override
    public void subscribe(PriceProcessor priceProcessor) {
            subscribers.add(priceProcessor);
    }

    @Override
    public void  unsubscribe(PriceProcessor priceProcessor) {

        subscribers.remove(priceProcessor);

        if (runningTasks.containsKey(priceProcessor)) {

            getDataToProcess(priceProcessor); //mark current data as processed

            var task = runningTasks.get(priceProcessor);

            try {
                task.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            runningTasks.remove(priceProcessor);
            subscriberData.remove(priceProcessor);
        }
    }

    private void setDataToProcess(PriceProcessor processor, TaskArguments arguments) {
        subscriberData.compute(processor, (key, val) -> {
                val = arguments;
                return val;
            });
    }

    private TaskArguments getDataToProcess(PriceProcessor processor) {
        return subscriberData.computeIfPresent(processor, (key, val) -> {
            val.setProcessed();
            return val;
        });
    }

    private Runnable createRunnable(PriceProcessor processor) {
        return () -> {
            while (!subscriberData.get(processor).isProcessed()) {
                var data = this.getDataToProcess(processor);
                processor.onPrice(data.getCcyPair(), data.getRate());
            }
        };
    }

    private void scheduleTask(PriceProcessor processor) {

        CompletableFuture<Void> task = null;

        if (runningTasks.containsKey(processor)) {
            task = runningTasks.get(processor);
        }

        if (task == null || task.isDone()) {
            var runnable = this.createRunnable(processor);
            task = CompletableFuture.runAsync(runnable);
            runningTasks.put(processor, task);
        }
    }
}
