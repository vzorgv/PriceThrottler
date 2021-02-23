package com.price.processor.throttler;

import java.util.HashMap;

/**
 * Implements the throttling strategy based on ranking of pair price.
 * Provides the equal chance to deliver pair prices regardless of their income frequency
 * The rank is defined as a Rank = DeliveredCount/IncomeCount
 * Rank is value defined on [0; 1].
 * Pair price with minimal rank is a the next to be returned
 * As pair price is returned, rank is recalculated to provide the equal chance to deliver
 */
final class DeliveryFreqRankThrottling implements ThrottlingStrategy {

    private final HashMap<String, PriceStatistics> reducedPairPrices = new HashMap<>();

    private static class PriceStatistics implements Comparable<PriceStatistics>{

        private CurrencyPairPrice pairPrice;
        private Long deliveredTotal;
        private Long incomeTotal;

        public PriceStatistics(CurrencyPairPrice pairPrice) {
            deliveredTotal = 0L;
            incomeTotal = 0L;
            this.pairPrice = pairPrice;
        }

        public CurrencyPairPrice getPairPrice() {
            return pairPrice;
        }

        public void setPairPrice(CurrencyPairPrice pairPrice) {
            this.pairPrice = pairPrice;
        }

        public float getDeliveredTotal() {
            return deliveredTotal;
        }

        public void setDeliveredTotal(Long deliveredTotal) {
            this.deliveredTotal = deliveredTotal;
        }

        public Long getIncomeTotal() {
            return incomeTotal;
        }

        public void setIncomeTotal(Long incomeTotal) {
            this.incomeTotal = incomeTotal;
        }

        public Float getRank() {
            return getIncomeTotal() != 0F
                    ? getDeliveredTotal() / getIncomeTotal()
                    : 0F;
        }

        public boolean isHighestRank() {
            return getRank() == 1F;
        }

        public void resetForHighestRank() {
            deliveredTotal = incomeTotal;
        }

        @Override
        public int compareTo(PriceStatistics o) {
            return this.getRank().compareTo(o.getRank());
        }
    }

    @Override
    public void pushItem(CurrencyPairPrice currencyPairPrice) {

        var statistics = reducedPairPrices.get(currencyPairPrice.getCcyPair());

        if (statistics == null) {
            statistics = new PriceStatistics(currencyPairPrice);
            statistics.setIncomeTotal(1L);
            reducedPairPrices.put(currencyPairPrice.getCcyPair(), statistics);
        } else {
            statistics.setIncomeTotal(statistics.getIncomeTotal() + 1);
            statistics.setPairPrice(currencyPairPrice);
        }
    }

    @Override
    public CurrencyPairPrice popItem() {

        var minRankPrice = findPriceWithMinRank();

        if (minRankPrice != null) {
            var minStat = reducedPairPrices.get(minRankPrice.getCcyPair());

            if (minStat.isHighestRank())
                minRankPrice = null;
            else
                minStat.resetForHighestRank();
        }

        return minRankPrice;
    }

    @Override
    public boolean isEmpty() {

        boolean ret = true;
        PriceStatistics minStat = null ;

        for (var currentStat : reducedPairPrices.values()) {

            if (minStat == null || minStat.compareTo(currentStat) > 0) {
                minStat = currentStat;
            }

            if (!minStat.isHighestRank()) {
                ret = false;
                break;
            }
        }

        return ret;
    }

    private CurrencyPairPrice findPriceWithMinRank() {
        CurrencyPairPrice minRankPrice = null;
        PriceStatistics minStat = null ;

        for (var currentStat : reducedPairPrices.values()) {

            if (minStat == null || minStat.compareTo(currentStat) > 0) {
                minStat = currentStat;
                minRankPrice = minStat.getPairPrice();
            }
        }

        return minRankPrice;
    }
}
