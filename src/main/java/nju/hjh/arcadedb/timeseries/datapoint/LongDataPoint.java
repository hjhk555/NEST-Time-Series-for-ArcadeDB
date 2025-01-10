package nju.hjh.arcadedb.timeseries.datapoint;

import com.arcadedb.database.Binary;
import nju.hjh.arcadedb.timeseries.UpdateStrategy;
import nju.hjh.arcadedb.timeseries.exception.DuplicateTimestampException;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

public class LongDataPoint extends DataPoint{
    public long value;

    public LongDataPoint() {}

    public LongDataPoint(long timestamp, long value){
        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public void serialize(Binary binary) {
        binary.putLong(timestamp);
        binary.putLong(value);
    }

    @Override
    public void deserialize(Binary binary) {
        timestamp = binary.getLong();
        value = binary.getLong();
    }

    @Override
    public DataPoint getUpdatedDataPoint(DataPoint income, UpdateStrategy strategy) throws TimeseriesException {
        if (income.timestamp != this.timestamp){
            throw new TimeseriesException("updating data point with different timestamp");
        }
        if (income instanceof LongDataPoint lIncome){
            switch (strategy.baseStrategy){
                case ERROR -> throw new DuplicateTimestampException(String.format("datapoint already exist at timestamp %d(=%d)", this.timestamp, this.value));
                case IGNORE -> {
                    return null;
                }
                case UPDATE -> {
                    return income;
                }
                case APPEND -> {
                    return new LongDataPoint(timestamp, this.value + lIncome.value);
                }
                default -> throw new TimeseriesException("unknown update strategy");
            }
        }else{
            throw new TimeseriesException("LongDataPoint can only be updated by LongDataPoint");
        }
    }

    public static int maxBytesRequired(){
        return 16;
    }

    @Override
    public int realBytesRequired() {
        return 16;
    }

    @Override
    public Object getValue() {
        return value;
    }
}
