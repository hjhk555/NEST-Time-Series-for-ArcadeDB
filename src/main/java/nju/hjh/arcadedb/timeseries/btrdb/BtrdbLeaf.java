package nju.hjh.arcadedb.timeseries.btrdb;

import com.arcadedb.database.*;
import nju.hjh.arcadedb.timeseries.DataPointList;
import nju.hjh.arcadedb.timeseries.datapoint.DoubleDataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

import java.util.Map;
import java.util.TreeMap;

public class BtrdbLeaf extends BtrdbNode{
    public static final byte NODE_TYPE = 2;
    public TreeMap<Long, Double> datapoints = new TreeMap<>();
    public BtrdbLeaf(long begin, long end, Database database, String metric, DoubleStatistics statistics) {
        super(begin, end, database, metric, statistics);
    }

    @Override
    public RID serializeIfDirty() {
        if (!dirty) return null;
        MutableDocument document = database.newDocument(metric);
        Binary binary = new Binary();
        binary.putByte(NODE_TYPE);
        binary.putLong(begin);
        binary.putLong(end);
        binary.putInt(datapoints.size());
        for(Map.Entry<Long, Double> datapoint : datapoints.entrySet()){
            binary.putLong(datapoint.getKey());
            binary.putLong(Double.doubleToLongBits(datapoint.getValue()));
        }
        document.set(PROP_NODE_BINARY, binary.toByteArray());
        document.save();
        dirty=false;
        return document.getIdentity();
    }

    @Override
    public void insertDatapoint(long timestamp, double value, InsertionCallback callback) throws TimeseriesException {
        if (timestamp < begin || timestamp > end) return;
        if (datapoints.get(timestamp) != null) return;
        datapoints.put(timestamp, value);
        if (statistics!=null) statistics.append(value);
        if (datapoints.size() <= LEAF_THRESHOLD) {
            callback.call(this);
            setAsDirty();
            return;
        }
        // turn into internal node
        BtrdbInternal newInternal = new BtrdbInternal(this);
        if (callback != null) callback.call(newInternal);
    }

    @Override
    public DoubleStatistics aggregationQuery(long startTime, long endTime) throws TimeseriesException {
        //System.out.println("leaf: "+begin+" -> "+end);
        if (datapoints.size() == 0) return DoubleStatistics.EMPTY_STATISTICS;
        if (startTime > this.end || endTime < this.begin) return DoubleStatistics.EMPTY_STATISTICS;
        if (datapoints.firstKey()>= startTime && datapoints.lastKey()<=endTime) return statistics.clone();

        DoubleStatistics ans = new DoubleStatistics();
        for(Map.Entry<Long, Double> entry : datapoints.subMap(startTime, true, endTime, true).entrySet()){
            ans.append(entry.getValue());
        }
        return ans;
    }

    @Override
    public void periodQuery(long startTime, long endTime, int limits, DataPointList list) {
        if (startTime > this.end || end < this.begin) return;
        int remains = limits - list.dataPointList.size();
        if (limits >=0 && remains <=0) return;
        for (Map.Entry<Long, Double> datapoint : datapoints.subMap(startTime, true, endTime, true).entrySet()){
            list.dataPointList.add(new DoubleDataPoint(datapoint.getKey(), datapoint.getValue()));
            if (limits >=0)
                if (--remains == 0) break;
        }
    }
}
