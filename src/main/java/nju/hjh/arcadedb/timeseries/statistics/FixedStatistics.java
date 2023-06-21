package nju.hjh.arcadedb.timeseries.statistics;

public abstract class FixedStatistics extends Statistics{
    public abstract Object getFirstValue();
    public abstract Object getLastValue();
}
