package com.price.processor.throttler;

/**
 * The throttling strategy
 * Describes the throttling
 */
public interface ThrottlingStrategy {
    /**
     * Adds value to throttling
     * @param currencyPairPrice the <c>CurrencyPairPrice</c> value
     */
    void pushItem(CurrencyPairPrice currencyPairPrice);

    /**
     * Fetched throttled value
     * @return The <c>CurrencyPairPrice</c> instance
     */
    CurrencyPairPrice popItem();

    /**
     * Whether the value are ready to fetch
     * @return True if there is a value to fetch otherwise False
     */
    boolean isEmpty();
}
