package nju.hjh.arcadedb.timeseries.datapoint;

import com.arcadedb.database.Binary;

public class LongDataPoint extends DataPoint{
    public long value;

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
    public int realBytesRequired() {
        return 16;
    }

    @Override
    public Object getValue() {
        return value;
    }
}
