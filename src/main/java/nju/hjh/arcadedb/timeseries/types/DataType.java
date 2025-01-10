package nju.hjh.arcadedb.timeseries.types;

import com.arcadedb.database.Binary;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.exception.MessageParsingException;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;

import java.math.BigDecimal;

public abstract class DataType {
    final byte seq;
    final String name;

    public static DataType resolveFromBinary(Binary binary) throws TimeseriesException {
        byte bType = binary.getByte();
        DataType type = switch (bType) {
            case 0 -> LongDataType.getInstance();
            case 1 -> DoubleDataType.getInstance();
            case 2 -> new StringDataType();
            default -> throw new TimeseriesException("invalid data type");
        };
        type.externalDeserialize(binary);
        return type;
    }

    public static DataType inferDatatype(Object value) throws TimeseriesException {
        if (value instanceof Number numVal) {
            if (numVal instanceof BigDecimal || numVal instanceof Double) {
                return DoubleDataType.getInstance();
            } else {
                return LongDataType.getInstance();
            }
        } else if (value instanceof String) {
            return new StringDataType();
        } else {
            throw new MessageParsingException("unsupported value ["+value+"]");
        }
    }

    public DataType(byte seq, String name){
        this.seq = seq;
        this.name = name;
    }

    public void serialize(Binary binary){
        binary.putByte(seq);
    }

    public int byteUsed(){
        return 1;
    }

    /**
     * deserialize additional params of data type
     */
    public void externalDeserialize(Binary binary){}

    /**
     * check if this data type is valid
     * @throws TimeseriesException if not valid
     */
    public abstract void checkValid() throws TimeseriesException;

    /**
     * @return true if this data type has fixed size
     */
    public abstract boolean isFixed();

    public abstract DataPoint convertValue2DataPoint(long timestamp, Object value) throws TimeseriesException;

    public abstract void checkDataPointValid(DataPoint dataPoint) throws TimeseriesException;

    public abstract int maxStatisticsBytes();

    public abstract Statistics newEmptyStatistics();

    public abstract int maxDataPointBytes();

    public abstract DataPoint newEmptyDataPoint();
}
