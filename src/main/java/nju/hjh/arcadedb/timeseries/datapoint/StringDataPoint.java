package nju.hjh.arcadedb.timeseries.datapoint;

import com.arcadedb.database.Binary;
import nju.hjh.arcadedb.timeseries.MathUtils;
import nju.hjh.arcadedb.timeseries.TSUpdateStrategy;
import nju.hjh.arcadedb.timeseries.exception.DuplicateTimestampException;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

public class StringDataPoint extends DataPoint{
    public static int MAX_LENGTH = 2000;
    public String value;

    public StringDataPoint(long timestamp, String value){
        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public void serialize(Binary binary) {
        binary.putLong(timestamp);
        binary.putString(value);
    }

    @Override
    public DataPoint getUpdatedDataPoint(DataPoint income, TSUpdateStrategy strategy) throws TimeseriesException {
        if (income.timestamp != this.timestamp){
            throw new TimeseriesException("updating data point with different timestamp");
        }
        if (income instanceof StringDataPoint sIncome){
            switch (strategy.baseStrategy){
                case ERROR -> throw new DuplicateTimestampException(String.format("datapoint already exist at timestamp %d(=%s)", this.timestamp, this.value));
                case IGNORE -> {
                    return null;
                }
                case UPDATE -> {
                    return income;
                }
                case APPEND -> {
                    String newString = this.value+strategy.separator+sIncome.value;
                    if (newString.length() > MAX_LENGTH)
                        throw new DuplicateTimestampException(String.format("string length exceeded hard limit(%d) after append", MAX_LENGTH));
                    return new StringDataPoint(timestamp, newString);
                }
                default -> throw new TimeseriesException("unknown update strategy");
            }
        }else{
            throw new TimeseriesException("StringDataPoint can only be updated by StringDataPoint");
        }
    }

    @Override
    public int realBytesRequired() {
        int strLen = value.length();
        return 8 + MathUtils.bytesToWriteUnsignedNumber(strLen) + strLen;
    }

    @Override
    public Object getValue() {
        return value;
    }
}
