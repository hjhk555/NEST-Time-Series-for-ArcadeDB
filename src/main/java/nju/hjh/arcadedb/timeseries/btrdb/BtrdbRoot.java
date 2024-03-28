package nju.hjh.arcadedb.timeseries.btrdb;

import com.arcadedb.database.RID;
import nju.hjh.arcadedb.timeseries.DataPointList;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

public class BtrdbRoot {
    public BtrdbNode rootNode;

    public BtrdbRoot(BtrdbNode rootNode) {
        this.rootNode = rootNode;
    }

    public RID commit(){
        if (rootNode == null) return null;
        return rootNode.serializeIfDirty();
    }

    public void insertDatapoint(long timestamp, double value) throws TimeseriesException {
        rootNode.insertDatapoint(timestamp, value, entry -> rootNode = entry);
    }

    public DoubleStatistics aggregationQuery(long startTime, long endTime) throws TimeseriesException {
        return rootNode.aggregationQuery(startTime, endTime);
    }

    public DataPointList periodQuery(long startTime, long endTime, int limits) throws TimeseriesException {
        DataPointList list = new DataPointList();
        rootNode.periodQuery(startTime, endTime, limits, list);
        return list;
    }
}
