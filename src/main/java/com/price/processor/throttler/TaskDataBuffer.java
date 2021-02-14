package com.price.processor.throttler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class TaskDataBuffer {
    private final static Logger logger = LogManager.getLogger(TaskDataBuffer.class);

    private final ConcurrentHashMap<String, TaskData> prices = new ConcurrentHashMap<>();

    private final Lock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final AtomicBoolean isCanceled = new AtomicBoolean(false);

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
