package nju.hjh.arcadedb.timeseries.datapoint;

import com.arcadedb.database.Binary;
import nju.hjh.arcadedb.timeseries.DataType;
import nju.hjh.arcadedb.timeseries.MathUtils;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

public abstract class DataPoint {
    public long timestamp;

    public static int maxBytesRequired(DataType type) throws TimeseriesException {
        if (!type.isFixed())
            throw new TimeseriesException("cannot get max bytes for not fixed data type");
        return switch (type.baseType){
            case LONG -> 16;
            case STRING -> 8 + MathUtils.bytesToWriteUnsignedNumber(type.param) + type.param;
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

    public abstract int realBytesRequired();

    public abstract Object getValue();
}
