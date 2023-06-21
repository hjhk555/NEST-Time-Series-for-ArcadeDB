package nju.hjh.arcadedb.timeseries.statistics;

public abstract class NumericStatistics extends FixedStatistics{
    public abstract Number getSum();
    public abstract Number getMaxValue();
    public abstract Number getMinValue();
}
