package nju.hjh.arcadedb.timeseries.datapoint;

import com.arcadedb.database.Binary;

public class DoubleDataPoint extends DataPoint{
    public double value;

    public DoubleDataPoint(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public void serialize(Binary binary) {
        binary.putLong(timestamp);
        binary.putLong(Double.doubleToLongBits(value));
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
