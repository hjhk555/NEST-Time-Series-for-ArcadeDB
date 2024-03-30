package nju.hjh.arcadedb.timeseries.statistics;

import com.arcadedb.database.Binary;
import nju.hjh.arcadedb.timeseries.DataType;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

import java.util.NavigableMap;

public abstract class Statistics {
    public long count;
    public long firstTime;
    public long lastTime;

    public Statistics(){
        count = 0;
        firstTime = Long.MAX_VALUE;
        lastTime = Long.MIN_VALUE;
    }

    public static Statistics getStatisticsFromBinary(DataType type, Binary binary) throws TimeseriesException {
        Statistics stats = newEmptyStats(type);
        stats.deserialize(binary);
        return stats;
    }

    public static Statistics newEmptyStats(DataType type) throws TimeseriesException {
        if (!type.isFixed()) return new UnfixedStatistics();
        return switch (type.baseType){
            case LONG -> new LongStatistics();
            case STRING -> new StringStatistics();
            case DOUBLE -> new DoubleStatistics();
            default -> throw new TimeseriesException("invalid data type");
        };
    }

    public static int maxBytesRequired(DataType type) throws TimeseriesException {
        if (!type.isFixed()) return UnfixedStatistics.maxBytesRequired();
        return switch (type.baseType){
            case LONG -> LongStatistics.maxBytesRequired();
            case STRING -> StringStatistics.maxBytesRequired(type.param);
            case DOUBLE -> DoubleStatistics.maxBytesRequired();
            default -> throw new TimeseriesException("invalid data type");
        };
    }
    public void clear(){
        count = 0;
        firstTime = Long.MAX_VALUE;
        lastTime = Long.MIN_VALUE;
    }

    // insert single dataPoint into statistics
    public abstract void insert(DataPoint data) throws TimeseriesException;

    /** try to update old dataPoint to new DataPoint without re-stats
     * @param oldDP old data point
     * @param newDP new data point
     * @return true if success, false if re-stats needed
     */
    public abstract boolean update(DataPoint oldDP, DataPoint newDP) throws TimeseriesException;
    // insert dataPoints into statistics
    public abstract void insertAll(NavigableMap<Long, DataPoint> dataList);
    // merge 2 statistics together
    public abstract void merge(Statistics stats) throws TimeseriesException;
    // serialize statistics into binary
    public abstract void serialize(Binary binary);
    // deserialize statistics from binary
    public abstract void deserialize(Binary binary);
    // get a deep-cloned statistics of this
    public abstract Statistics clone();
    // pretty print statistics
    public abstract String toPrettyPrintString();
}
