package nju.hjh.arcadedb.timeseries.statistics;

import com.arcadedb.database.Binary;
import nju.hjh.arcadedb.timeseries.DataType;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

import java.util.List;

public abstract class Statistics {
    public long count;
    public long firstTime;
    public long lastTime;

    public Statistics(){
        count = 0;
        firstTime = Long.MAX_VALUE;
        lastTime = Long.MIN_VALUE;
    }

    public static int bytesToWriteUnsignedNumber(long number){
        int bytes = 1;
        number >>>= 7;
        while (number != 0){
            bytes++;
            number >>>= 7;
        }
        return bytes;
    }

    public static Statistics getStatisticsFromBinary(DataType type, Binary binary) throws TimeseriesException {
        Statistics stats = newEmptyStats(type);
        stats.deserialize(binary);
        return stats;
    }

    public static Statistics newEmptyStats(DataType type) throws TimeseriesException {
        switch (type.baseType){
            case LONG -> {
                return new LongStatistics();
            }
            case STRING -> {
                return new StringStatistics();
            }
            default -> {
                throw new TimeseriesException("invalid data type");
            }
        }
    }

    public static Statistics countStats(DataType type, List<DataPoint> dataList, boolean isTimeOrdered) throws TimeseriesException {
        if (dataList.size() == 0)
            return null;

        Statistics stats = newEmptyStats(type);
        stats.insertDataList(dataList, isTimeOrdered);
        return stats;
    }

    public static int bytesToWrite(DataType type) throws TimeseriesException {
        return switch (type.baseType){
            case LONG -> LongStatistics.bytesToWrite();
            case STRING -> StringStatistics.bytesToWrite(type.param);
            default -> throw new TimeseriesException("invalid data type");
        };
    }

    // insert single dataPoint into statistics
    public abstract void insert(DataPoint data) throws TimeseriesException;

    /** try to update old dataPoint to new DataPoint without re-stats
     * @param oldDP old data point
     * @param newDP new data point
     * @return true if success, false if re-stats needed
     */
    public abstract boolean update(DataPoint oldDP, DataPoint newDP) throws TimeseriesException;
    // insert list of dataPoints into statistics
    public abstract void insertDataList(List<DataPoint> dataList, boolean isTimeOrdered);
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
