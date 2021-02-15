package com.price.processor.throttler;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class CurrencyPriceQueueTest {

    @Test
    public void eventShouldBeOfferedWithSuccess() {
        var buffer = new CurrencyPriceQueue();
        var actual = buffer.offer(new TaskData("AAA", 0.11));

        try {
            buffer.close();
        } catch (Exception e) {
            //Do nothing
        }

        assertTrue(actual);
    }
}
