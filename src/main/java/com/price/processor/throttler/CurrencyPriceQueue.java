package com.price.processor.throttler;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class CurrencyPriceQueue implements AutoCloseable {

    private final static Logger logger = LogManager.getLogger(CurrencyPriceQueue.class);

    private final ConcurrentHashMap<String, TaskData> prices = new ConcurrentHashMap<>();

    private final Lock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final AtomicBoolean isCanceled = new AtomicBoolean(false);




    private final static int EVENT_QUEUE_CAPACITY = 1;
    //TODO: extract termination event
    public final static String TERMINATION_EVENT_ID = UUID.randomUUID().toString();

    //TODO: redesign as <Event, Integer>
    HashMap<String, EventCounter> bufferedEvents = new HashMap<>();

    //TODO: validate queue with several fairness
    ArrayBlockingQueue<TaskData> eventQueue = new ArrayBlockingQueue<>(EVENT_QUEUE_CAPACITY, false);
    private CompletableFuture<Void> bufferWatcherTask;
    private final LinkedBlockingQueue<TaskData> watcherQueue = new LinkedBlockingQueue<>();

    private class EventCounter {

        private final TaskData data;
        private Integer count;

        public EventCounter(TaskData event) {
            data = event;
            count = 0;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        public TaskData getData() {
            return data;
        }
    }

    public CurrencyPriceQueue() {
        startWatcher();
    }

    @Override
    public void close() throws Exception {
        if (!bufferWatcherTask.isDone()) {
            watcherQueue.offer(new TaskData(TERMINATION_EVENT_ID, 0.0));
            bufferWatcherTask.join();
            logger.info(String.format("Queue %s closed", this.toString()));
        }
    }

    public boolean offer(TaskData priceEvent) {
        var ret = watcherQueue.offer(priceEvent);
        return ret;
    }

    public TaskData take() {
        TaskData event = null;
        try {
            event = eventQueue.take();
        } catch (InterruptedException e) {
            //Do nothing
        }
        return event;
    }

    private void startWatcher() {
        bufferWatcherTask = CompletableFuture.runAsync(() -> {
            try {
                boolean running = true;
                do {
                    TaskData event;

                    if (bufferedEvents.isEmpty()) {
                        event = watcherQueue.take();
                    } else {
                        event = watcherQueue.poll();
                    }

                    if (isTerminationEvent(event))
                        running = false;

                    addEventToBuffer(event);

                    //TODO: get rare event to enqueue from buffer
                    eventQueue.put(event); //TODO: block insertion
                    //if (eventQueue.offer(event)) {
                        logger.info(String.format("Pair %s %f put to event queue", event.getCcyPair(), event.getRate()));
                        bufferedEvents.remove(event.getCcyPair());
                    //}
                } while (running);

            } catch (InterruptedException e) {
                //Do nothing
            }
        });
    }

    private boolean isTerminationEvent(TaskData event) {
        return TERMINATION_EVENT_ID.equals(event.getCcyPair());
    }

    private void addEventToBuffer(TaskData priceEvent) {
        bufferedEvents.compute(priceEvent.getCcyPair(), (key, val) -> {
            if (val == null) {
                val = new EventCounter(priceEvent);
            } else {
                val.setCount(val.getCount() + 1);
                val.getData().setRate(priceEvent.getRate());
            }
            return val;
        });
    }





    //TODO: obsolete
    public void put(String ccyPair, double rate) {

        lock.lock();
        try {
            setDataToProcess(ccyPair, rate);
            logger.info(String.format("Put %s %f", ccyPair, rate));
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /*
    public TaskData take() {

        TaskData ret = null;

        lock.lock();
        try {
            while (!isCanceled.getAcquire() && !isDataReady())
                notEmpty.await();

            if (!isCanceled.getAcquire()) {
                ret = getDataToProcess();
                logger.info(String.format("Take %s %f", ret.getCcyPair(), ret.getRate()));
            }

        } catch (InterruptedException e) {

        } finally {
            lock.unlock();
        }

        return ret;
    }
     */

    //TODO: consider to remove
    public void cancel() {
        lock.lock();
        try {
            isCanceled.setRelease(true);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    private void setDataToProcess(String ccyPair, double rate) {

        prices.compute(ccyPair, (key, val) -> {
            if (val == null) {
                val = new TaskData(ccyPair, rate);
            } else {
                val.setProcessed(false);
                val.setRate(rate);
            }
            return val;
        });
    }

    private TaskData getDataToProcess() {

        var ccyPair = findAvailableData();

        return prices.computeIfPresent(ccyPair, (key, val) -> {
                val.setProcessed(true);
                return val;
        });
    }

    private boolean isDataReady() {
        var ret = prices.values()
                .stream()
                .filter(x -> !x.isProcessed())
                .findAny();
        return ret.isPresent();
    }

    private String findAvailableData() {
        return prices.values()
                .stream()
                .filter(x -> !x.isProcessed())
                .findAny()
                .get()
                .getCcyPair();
    }
}
