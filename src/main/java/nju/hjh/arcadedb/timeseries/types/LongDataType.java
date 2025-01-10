package nju.hjh.arcadedb.timeseries.types;

import lombok.Getter;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.datapoint.LongDataPoint;
import nju.hjh.arcadedb.timeseries.exception.DataTypeMismatchException;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.LongStatistics;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;

public class LongDataType extends DataType{
    @Getter
    private final static LongDataType instance = new LongDataType();

    private LongDataType(){
        super((byte) 0, "LONG");
    }

    @Override
    public void checkValid() throws TimeseriesException {
    }

    @Override
    public boolean isFixed() {
        return true;
    }

    @Override
    public DataPoint convertValue2DataPoint(long timestamp, Object value) throws TimeseriesException{
        long l;
        if (value instanceof Number number)
            l = number.longValue();
        else if (value instanceof String str)
            try {
                l = Long.parseLong(str);
            } catch (NumberFormatException e) {
                throw new DataTypeMismatchException("unmatched value ["+value+"] and type(Long)");
            }
        else
            throw new DataTypeMismatchException("unmatched value ["+value+"] and type(Long)");
        return new LongDataPoint(timestamp, l);
    }

    @Override
    public void checkDataPointValid(DataPoint dataPoint) throws TimeseriesException {
        if (dataPoint instanceof LongDataPoint){
        }else{
            throw new TimeseriesException("data point type is not long");
        }
    }

    @Override
    public int maxStatisticsBytes() {
        return LongStatistics.maxBytesRequired();
    }

    @Override
    public Statistics newEmptyStatistics() {
        return new LongStatistics();
    }

    @Override
    public int maxDataPointBytes() {
        return LongDataPoint.maxBytesRequired();
    }

    @Override
    public DataPoint newEmptyDataPoint() {
        return new LongDataPoint();
    }
}
