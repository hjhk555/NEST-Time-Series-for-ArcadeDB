package nju.hjh.arcadedb.timeseries.datapoint;

import com.arcadedb.database.Binary;
import nju.hjh.arcadedb.timeseries.types.DataType;
import nju.hjh.arcadedb.timeseries.MathUtils;
import nju.hjh.arcadedb.timeseries.UpdateStrategy;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

public abstract class DataPoint {
    public long timestamp;

    public abstract void serialize(Binary binary);
    public abstract void deserialize(Binary binary);
    public abstract DataPoint getUpdatedDataPoint(DataPoint income, UpdateStrategy strategy) throws TimeseriesException;
    public abstract int realBytesRequired();
    public abstract Object getValue();
}
