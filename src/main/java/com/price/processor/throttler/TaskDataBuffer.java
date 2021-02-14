package com.price.processor.throttler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class TaskDataBuffer {
    private final ConcurrentHashMap<String, TaskData> prices = new ConcurrentHashMap<>();

    private final Lock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final AtomicBoolean isCanceled = new AtomicBoolean(false);

    public void put(String ccyPair, double rate) {

        lock.lock();
        try {
            setDataToProcess(ccyPair, rate);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    public TaskData take() {

        TaskData ret = null;

        lock.lock();
        try {
            while (!isCanceled.getAcquire() && !isDataReady())
                notEmpty.await();

            if (!isCanceled.getAcquire()) {
                ret = prices.values()
                        .stream()
                        .filter(x -> !x.isProcessed())
                        .findAny()
                        .get();
//TODO: consider case with empty stream
                ret = getDataToProcess(ret.getCcyPair());
            }

        } catch (InterruptedException e) {

        } finally {
            lock.unlock();
        }

        return ret;
    }

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

    private TaskData getDataToProcess(String ccyPair) {

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
}
