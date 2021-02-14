package com.price.processor;

import com.price.processor.model.Price;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class Subscription {
    private final ConcurrentHashMap<String, Price> prices = new ConcurrentHashMap<>();

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

    public Price take() {

        Price ret = null;

        lock.lock();
        try {
            while (!isCanceled.get() && !isDataReady())
                notEmpty.await();

            if (!isCanceled.get()) {
                ret = prices.values()
                        .stream()
                        .filter(x -> x.isProcessed() == false)
                        .findAny()
                        .get();
//TODO: consider with empty stream
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
            isCanceled.getAndSet(true);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    private void setDataToProcess(String ccyPair, double rate) {

        prices.compute(ccyPair, (key, val) -> {
            if (val == null) {
                val = new Price(ccyPair, rate);
            } else {
                val.setProcessed(false);
                val.setRate(rate);
            }
            return val;
        });
    }

    private Price getDataToProcess(String ccyPair) {

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
