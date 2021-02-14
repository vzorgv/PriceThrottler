package com.price.processor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class Subscription {
    private final ConcurrentHashMap<String, Price> prices = new ConcurrentHashMap<>();

    private final Lock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private boolean isCanceled = false;

    public void put(String ccyPair, double rate) {
        lock.lock();
        try {
            setDataToProcess(ccyPair, rate);
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public Price take() {

        Price ret = null;

        lock.lock();
        try {

            while (!isCanceled && !isDataReady())
                notEmpty.await();

            if (!isCanceled) {
                ret = prices.values()
                        .stream()
                        .filter(x -> x.isProcessed() == false)
                        .findAny()
                        .get();

                ret = getDataToProcess(ret.getCcyPair());
            }

        } catch (InterruptedException e) {

        } finally {
            lock.unlock();
        }

        return ret;
    }

    public void cancel() {
        lock.lock();
        try {
            isCanceled = true;
            notEmpty.signalAll();
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
                .filter(x -> x.isProcessed() == false)
                .findAny();
        return ret.isPresent();
    }
}
