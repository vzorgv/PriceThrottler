package com.price.processor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;


public class PriceThrottlerTest {

    @Test
    @DisplayName("When event comes then distributed to listeners")
    public void eventShouldBeDistributedToListeners() {

    }


    @Test
    @DisplayName("ONLY LAST PRICE for each ccyPair matters for subscribers")
    public void theLastPriceShouldBeProcessed() {

    }

    @Test
    @DisplayName("Slow subscribers should not impact fast subscribers")
    public void theTwoProccessorsShouldNotBlockEachOther() {

    }

    @Test
    @DisplayName("Not to miss rarely changing prices")
    public void theAllPricesShouldBeDelivered() {

    }

    @Test
    @DisplayName("Unsubscribe for running processor should be executed successfully")
    public void theRunningSubscriberShouldBeUnsubscribeSuccessfully() {

    }

    @Test
    @DisplayName("Unsubscribe for idle processor")
    public void theIdleSubscriberShouldBeUnsubscribeSuccessfully() {

    }


    @Test
    @DisplayName("Unsubscribe for not existed processor")
    public void theNotSubscribedProcessorShouldNotThrowException() {

    }
}