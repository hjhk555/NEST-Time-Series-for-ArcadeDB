package nju.hjh.arcadedb.timeseries.types;

import com.arcadedb.database.Binary;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.datapoint.StringDataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.FixedStatistics;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;
import nju.hjh.arcadedb.timeseries.statistics.StringStatistics;
import nju.hjh.arcadedb.timeseries.statistics.UnfixedStatistics;

public class StringDataType extends DataType{
    private int maxLength;

    public StringDataType() {
        this(0);
    }

    public StringDataType(int maxLength) {
        super((byte) 2, "STRING");
        this.maxLength = maxLength;
    }

    public int getMaxLength() {
        return maxLength;
    }

    @Override
    public void checkValid() throws TimeseriesException {
        if (maxLength < 0) throw new TimeseriesException("length of string should be 0 or positive");
        if (maxLength > StringDataPoint.MAX_LENGTH) throw new TimeseriesException("length of string should not exceed "+StringDataPoint.MAX_LENGTH);
    }

    @Override
    public int byteUsed() {
        return 5;
    }

    @Override
    public void serialize(Binary binary) {
        super.serialize(binary);
        binary.putInt(maxLength);
    }

    @Override
    public void externalDeserialize(Binary binary) {
        super.externalDeserialize(binary);
        maxLength = binary.getInt();
    }

    @Override
    public boolean isFixed() {
        return maxLength > 0;
    }

    private void isValueValid(String value) throws TimeseriesException {
        int strLen = value.length();
        if (strLen > StringDataPoint.MAX_LENGTH)
            throw new TimeseriesException(String.format("string length(%d) exceeded hard limit(%d)", strLen, StringDataPoint.MAX_LENGTH));
        if (maxLength > 0 && strLen > maxLength)
            throw new TimeseriesException(String.format("string lwngth(%d) exceeded limit(%d)", strLen, maxLength));
    }

    @Override
    public DataPoint convertValue2DataPoint(long timestamp, Object value) throws TimeseriesException {
        String str = value.toString();
        isValueValid(str);
        return new StringDataPoint(timestamp, str);
    }

    @Override
    public void checkDataPointValid(DataPoint dataPoint) throws TimeseriesException {
        if (dataPoint instanceof StringDataPoint strPoint){
            isValueValid(strPoint.value);
        }else{
            throw new TimeseriesException("data point type is not string");
        }
    }

    @Override
    public int maxStatisticsBytes() {
        if (isFixed()) return StringStatistics.maxBytesRequired(maxLength);
        return UnfixedStatistics.maxBytesRequired();
    }

    @Override
    public Statistics newEmptyStatistics() {
        if (isFixed()) return new StringStatistics();
        return new UnfixedStatistics();
    }

    @Override
    public int maxDataPointBytes() {
        return StringDataPoint.maxBytesRequired(maxLength);
    }

    @Override
    public DataPoint newEmptyDataPoint() {
        return new StringDataPoint();
    }
}
