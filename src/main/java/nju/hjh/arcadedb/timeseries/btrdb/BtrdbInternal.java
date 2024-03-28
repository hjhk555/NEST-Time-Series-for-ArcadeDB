package nju.hjh.arcadedb.timeseries.btrdb;

import com.arcadedb.database.*;
import nju.hjh.arcadedb.timeseries.DataPointList;
import nju.hjh.arcadedb.timeseries.btrdb.DoubleStatistics;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

import java.awt.*;
import java.util.Map;

public class BtrdbInternal extends BtrdbNode{
    public final long gap;
    public static final byte NODE_TYPE = 1;

    public static class Child{
        public Child(BtrdbNode node, DoubleStatistics statistics, RID rid) {
            this.node = node;
            this.statistics = statistics;
            this.rid = rid;
        }

        public BtrdbNode node;
        public DoubleStatistics statistics;
        public RID rid;
    }
    public final Child[] children = new Child[BTRDB_DEGREE];

    public BtrdbInternal(long begin, long end, Database database, String metric, DoubleStatistics statistics) {
        super(begin, end, database, metric, statistics);
        gap = ((end-begin)>>6)+1;
    }

    public BtrdbInternal(BtrdbLeaf leaf) throws TimeseriesException {
        this(leaf.begin, leaf.end, leaf.database, leaf.metric, leaf.statistics);

        for(Map.Entry<Long, Double> datapoint : leaf.datapoints.entrySet()){
            insertDatapoint(datapoint.getKey(), datapoint.getValue(), null, false);
        }
    }

    @Override
    public RID serializeIfDirty() {
        if (!dirty) return null;
        MutableDocument document = database.newDocument(metric);

        Binary binary = new Binary();
        binary.putByte(NODE_TYPE);
        binary.putLong(begin);
        binary.putLong(end);
        for(Child child : children){
            if (child == null){
                // write null rid
                binary.putInt(-1);
                binary.putLong(-1L);
                continue;
            }

            if (child.node != null) {
                RID newChildRID = child.node.serializeIfDirty();
                if (newChildRID != null) child.rid = newChildRID;
            }
            if (child.rid == null || !child.rid.isValid()){
                // write null rid
                binary.putInt(-1);
                binary.putLong(-1L);
                continue;
            }

            binary.putInt(child.rid.getBucketId());
            binary.putLong(child.rid.getPosition());
            child.statistics.serialize(binary);
        }
        document.set(PROP_NODE_BINARY, binary.toByteArray());
        document.save();
        dirty = false;
        return document.getIdentity();
    }

    @Override
    public void insertDatapoint(long timestamp, double value, InsertionCallback callback) throws TimeseriesException {
        insertDatapoint(timestamp, value, callback, true);
    }

    @Override
    public DoubleStatistics aggregationQuery(long startTime, long endTime) throws TimeseriesException {
        //System.out.println("internal: "+begin+" -> "+end);
        if (startTime > this.end || endTime < this.begin) return DoubleStatistics.EMPTY_STATISTICS;
        if (startTime <= this.begin && endTime >= this.end && this.statistics != null) return this.statistics.clone();

        DoubleStatistics ans = new DoubleStatistics();
        int beginIdx = startTime < this.begin ? 0 : (int) ((startTime - this.begin)/gap);
        int endIdx = endTime > this.end ? BTRDB_DEGREE-1 : (int) ((endTime - this.begin) / gap);
        for(int i=beginIdx+1; i<endIdx; i++) if (children[i] != null) ans.append(children[i].statistics);
        // begin
        if (children[beginIdx]!=null){
            if (startTime <= nodeBegin(beginIdx) && endTime >= nodeEnd(beginIdx))
                ans.append(children[beginIdx].statistics);
            else {
                if (children[beginIdx].node == null) loadNode(beginIdx);
                ans.append(children[beginIdx].node.aggregationQuery(startTime, endTime));
            }
        }
        // end
        if (beginIdx != endIdx && children[endIdx]!=null){
            if (startTime <= nodeBegin(endIdx) && endTime >= nodeEnd(endIdx))
                ans.append(children[endIdx].statistics);
            else {
                if (children[endIdx].node == null) loadNode(endIdx);
                ans.append(children[endIdx].node.aggregationQuery(startTime, endTime));
            }
        }
        return ans;
    }

    @Override
    public void periodQuery(long startTime, long endTime, int limits, DataPointList list) throws TimeseriesException {
        if (limits >=0 && list.dataPointList.size() >= limits) return;
        if (startTime > this.end || endTime < this.begin) return;

        int beginIdx = startTime < this.begin ? 0 : (int) ((startTime - this.begin)/gap);
        int endIdx = endTime > this.end ? BTRDB_DEGREE-1 : (int) ((endTime - this.begin) / gap);
        for(int i = beginIdx; i<=endIdx; i++){
            if (children[i] == null) continue;
            if (children[i].node == null) loadNode(i);
            children[i].node.periodQuery(startTime, endTime, limits, list);
            if (limits >=0 && list.dataPointList.size() >= limits) break;
        }
    }

    private void loadNode(int index) throws TimeseriesException {
        if (children[index] == null) return;
        children[index].node = BtrdbNode.getBtrdbNode(database, children[index].rid, metric, children[index].statistics);
    }

    private long nodeBegin(int index){
        return begin+index*gap;
    }

    private long nodeEnd(int index){
        return begin-1+(index+1)*gap;
    }

    public void insertDatapoint(long timestamp, double value, InsertionCallback callback, boolean appendStatistics) throws TimeseriesException {
        if (timestamp < begin || timestamp > end) return;
        int index = (int) ((timestamp-begin)/gap);
        if (children[index] == null){
            // create one
            DoubleStatistics newStatistics = new DoubleStatistics();
            BtrdbLeaf newLeaf = new BtrdbLeaf(nodeBegin(index), nodeEnd(index), database, metric, newStatistics);
            newLeaf.setAsDirty();
            children[index] = new Child(newLeaf, newStatistics, null);
        }else if (children[index].node == null) loadNode(index);
        children[index].node.insertDatapoint(timestamp, value, entry -> {
            children[index].node = entry;
            if (appendStatistics && statistics != null) statistics.append(value);
            setAsDirty();
        });
        if (callback != null) callback.call(this);
    }
}
