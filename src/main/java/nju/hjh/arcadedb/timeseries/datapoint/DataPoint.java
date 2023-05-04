package nju.hjh.arcadedb.timeseries.datapoint;

import com.arcadedb.database.Binary;
import nju.hjh.arcadedb.timeseries.DataType;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

public abstract class DataPoint {
    public long timestamp;

    public static int bytesToWrite(DataType type) throws TimeseriesException {
        return switch (type.baseType){
            case LONG -> 16;
            case STRING -> 8 + Statistics.bytesToWriteUnsignedNumber(type.param) + type.param;
            default -> throw new TimeseriesException("invalid data type");
        };
    }

    public static DataPoint getDataPointFromBinary(DataType dataType, Binary binary) throws TimeseriesException {
        return switch (dataType.baseType){
            case LONG -> new LongDataPoint(binary.getLong(), binary.getLong());
            case STRING -> new StringDataPoint(binary.getLong(), binary.getString());
            default -> throw new TimeseriesException("invalid data type");
        };
    }

    public abstract void serialize(Binary binary);

    public abstract Object getValue();
}
