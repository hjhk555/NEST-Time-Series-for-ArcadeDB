package nju.hjh.arcadedb.timeseries.datapoint;

import com.arcadedb.database.Binary;
import nju.hjh.arcadedb.timeseries.MathUtils;

public class StringDataPoint extends DataPoint{
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
    public int realBytesRequired() {
        int strLen = value.length();
        return 8 + MathUtils.bytesToWriteUnsignedNumber(strLen) + strLen;
    }

    @Override
    public Object getValue() {
        return value;
    }
}
