package nju.hjh.arcadedb.timeseries.types;

import lombok.Getter;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.datapoint.DoubleDataPoint;
import nju.hjh.arcadedb.timeseries.exception.DataTypeMismatchException;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.DoubleStatistics;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;

public class DoubleDataType extends DataType{
    @Getter
    private static final DoubleDataType instance = new DoubleDataType();

    private DoubleDataType() {
        super((byte) 1, "DOUBLE");
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
        double d;
        if (value instanceof Number number)
            d = number.doubleValue();
        else if (value instanceof String str)
            try {
                d = Double.parseDouble(str);
            } catch (NumberFormatException e){
                throw new DataTypeMismatchException("unmatched value ["+value+"] and type(Double)");
            }
        else
            throw new DataTypeMismatchException("unmatched value ["+value+"] and type(Double)");
        return new DoubleDataPoint(timestamp, d);
    }

    @Override
    public void checkDataPointValid(DataPoint dataPoint) throws TimeseriesException {
        if (dataPoint instanceof DoubleDataPoint){
        }else{
            throw new TimeseriesException("data point type is not double");
        }
    }

    @Override
    public int maxStatisticsBytes() {
        return DoubleStatistics.maxBytesRequired();
    }

    @Override
    public Statistics newEmptyStatistics() {
        return new DoubleStatistics();
    }

    @Override
    public int maxDataPointBytes() {
        return DoubleDataPoint.maxBytesRequired();
    }

    @Override
    public DataPoint newEmptyDataPoint() {
        return new DoubleDataPoint();
    }
}
