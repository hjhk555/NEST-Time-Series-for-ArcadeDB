package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.TreeMap;

public class NestNodeLeaf extends NestNode {
    public static final byte BLOCK_TYPE = 2;

    /** size of stat header:
     *  block type(1B) + nextRID(12B) + datapointCount(4B)
     */
    public static final int HEADER_SIZE = 17;

    public TreeMap<Long, DataPoint> datapoints;
    public RID succRID;
    // for unfixed data type
    public int dataBytesUsed;

    public NestNodeLeaf(Document document, String documentType, int degree, DataType dataType, long beginTimestamp, long endTimestamp, Statistics statistics) throws TimeseriesException {
        super(document, documentType, degree, dataType, beginTimestamp, endTimestamp, statistics);
    }

    @Override
    public void serializeIfDirty() throws TimeseriesException {
        if (!dirty) return;
        int binSize = HEADER_SIZE + MAX_DATA_BLOCK_SIZE;

        MutableDocument mutableDocument = document.modify();
        // put stat
        Binary binary = new Binary(binSize, false);
        binary.putByte(BLOCK_TYPE);
        binary.putInt(succRID.getBucketId());
        binary.putLong(succRID.getPosition());
        binary.putInt(datapoints.size());

        // put data
        for (DataPoint dataPoint : datapoints.values())
            dataPoint.serialize(binary);

        if (binary.size() > binSize)
            throw new TimeseriesException("stat header size exceeded");

        binary.size(binSize);
        mutableDocument.set(PROP_NODE_BINARY, binary.toByteArray());
        mutableDocument.save();
    }

    @Override
    public void insert(DataPoint data, UpdateStrategy strategy, InsertionCallback insertionCallback, UpdateCallback updateCallback, SplitCallback splitCallback) throws TimeseriesException {
        if (data.timestamp < beginTimestamp || data.timestamp > endTimestamp)
            throw new TimeseriesException("target dataPoint should not be handled by this leaf");

        if (dataType.isFixed())
            insertFixed(data, strategy, insertionCallback, updateCallback, splitCallback);
        else{
            insertUnfixed(data, strategy, insertionCallback, updateCallback, splitCallback);
        }
    }

    @Override
    public void insertLeafToTree(NestNodeLeaf leaf, BatchInsertionCallback batchInsertionCallback, ReachLeafCallback reachLeafCallback, SplitCallback splitCallback) throws TimeseriesException {
        reachLeafCallback.call();
    }

    public void insertFixed(DataPoint data, UpdateStrategy strategy, InsertionCallback insertionCallback, UpdateCallback updateCallback, SplitCallback splitCallback) throws TimeseriesException {
        int maxdataSize = MAX_DATA_BLOCK_SIZE / DataPoint.maxBytesRequired(dataType);

        DataPoint oldDP;
        if ((oldDP = datapoints.get(data.timestamp)) != null){
            // already exist
            DataPoint newDP = oldDP.getUpdatedDataPoint(data, strategy);
            if (newDP == null)
                return; // no need to update
            newDP = dataType.checkAndConvertDataPoint(newDP);
            if (oldDP.getValue().equals(newDP.getValue()))
                return; // no need to update
            datapoints.put(data.timestamp, newDP);
            if (statistics !=null && !statistics.update(oldDP, newDP)){
                statistics.clear();
                statistics.insertAll(datapoints);
            }
            if (updateCallback != null) updateCallback.call(oldDP, newDP);
        }else {
            // add new datapoint
            datapoints.put(data.timestamp, data);
            if (statistics != null) statistics.insert(data);
            if (insertionCallback != null) insertionCallback.call(data);
        }

        // split if full
        if (datapoints.size() > maxdataSize){
            // split into 2 blocks
            int totalSize = datapoints.size();
            int splitedSize = Math.min(maxdataSize, totalSize*(!succRID.isValid() ? LATEST_SPLIT_RATIO : OLD_SPLIT_RATIO)/100);

            TreeMap<Long, DataPoint> newDatapoints = new TreeMap<>();
            for(int i=0; i<totalSize-splitedSize; i++){
                DataPoint transPoint = datapoints.pollLastEntry().getValue();
                newDatapoints.put(transPoint.timestamp, transPoint);
            }
            long splitTime = newDatapoints.firstKey();
            MutableDocument newDoc = document.getDatabase().newDocument(documentType);
            NestNodeLeaf newLeaf = new NestNodeLeaf(newDoc, documentType, degree, dataType, splitTime, endTimestamp, Statistics.newEmptyStats(dataType));
            newLeaf.dirty = true;
            newLeaf.datapoints = newDatapoints;
            newLeaf.statistics.insertAll(newLeaf.datapoints);

            // update this leaf
            endTimestamp = splitTime-1;
            if (statistics != null) {
                statistics.clear();
                statistics.insertAll(datapoints);
            }

            // link leaves
            newLeaf.succRID = this.succRID;
            newLeaf.serializeIfDirty();
            this.succRID = newLeaf.document.getIdentity();

            if (splitCallback != null) splitCallback.call(newLeaf);
        }
        dirty = true;
    }

    private void insertUnfixed(DataPoint data, UpdateStrategy strategy, InsertionCallback insertionCallback, UpdateCallback updateCallback, SplitCallback splitCallback) throws TimeseriesException {
        DataPoint oldDP;
        if ((oldDP = datapoints.get(data.timestamp)) != null){
            // already exsit
            DataPoint newDP = oldDP.getUpdatedDataPoint(data, strategy);
            if (newDP == null)
                return; // no need to update
            newDP = dataType.checkAndConvertDataPoint(newDP);
            if (oldDP.getValue().equals(newDP.getValue()))
                return; // no need to update
            datapoints.put(newDP.timestamp, newDP);
            dataBytesUsed += newDP.realBytesRequired() - oldDP.realBytesRequired();
            if (statistics != null && !statistics.update(oldDP, newDP)){
                statistics.clear();
                statistics.insertAll(datapoints);
            }
            if (updateCallback != null) updateCallback.call(oldDP, newDP);
        }else {
            datapoints.put(data.timestamp, data);
            dataBytesUsed += data.realBytesRequired();
            if (statistics!=null) statistics.insert(data);
            if (insertionCallback != null) insertionCallback.call(data);
        }

        // check if require split, to achieve split, each data point should use at most MAX_DATA_BLOCK_SIZE/2 bytes
        if (dataBytesUsed > MAX_DATA_BLOCK_SIZE){
            // locate split size
            int splitedBytes, targetBytes = Math.min(MAX_DATA_BLOCK_SIZE, dataBytesUsed*(!succRID.isValid() ? LATEST_SPLIT_RATIO : OLD_SPLIT_RATIO)/100);
            // fill this block as much as possible
            splitedBytes = dataBytesUsed;
            TreeMap<Long, DataPoint> newDatapoints = new TreeMap<>();
            while (splitedBytes > targetBytes){
                DataPoint transPoint = datapoints.pollLastEntry().getValue();
                int transBytes = transPoint.realBytesRequired();
                // check if new leaf can hold this point
                if (dataBytesUsed-splitedBytes+transBytes>MAX_DATA_BLOCK_SIZE){
                    // send it back
                    datapoints.put(transPoint.timestamp, transPoint);
                    break;
                }
                newDatapoints.put(transPoint.timestamp, transPoint);
                splitedBytes -= transBytes;
            }
            long splitTime = newDatapoints.firstKey();

            // create latter leaf node
            MutableDocument newDoc = document.getDatabase().newDocument(documentType);
            NestNodeLeaf newLeaf = new NestNodeLeaf(newDoc, documentType, degree, dataType, splitTime, endTimestamp, Statistics.newEmptyStats(dataType));
            newLeaf.dirty = true;
            newLeaf.datapoints = newDatapoints;
            newLeaf.statistics.insertAll(newLeaf.datapoints);
            newLeaf.dataBytesUsed = this.dataBytesUsed - splitedBytes;
            newLeaf.serializeIfDirty();

            // update this leaf
            endTimestamp = splitTime-1;
            if (statistics!=null) {
                statistics.clear();
                statistics.insertAll(datapoints);
            }
            this.dataBytesUsed = splitedBytes;

            // link leaves
            newLeaf.succRID = this.succRID;
            this.succRID = newLeaf.document.getIdentity();

            if (splitCallback != null) splitCallback.call(newLeaf);
        }
        dirty = true;
    }

    // use the result as READ_ONLY !!!
    @Override
    public Statistics aggregativeQuery(long startTime, long endTime) throws TimeseriesException {
        if (statistics != null && startTime <= statistics.firstTime && endTime >= statistics.lastTime)
            return statistics;

        Statistics result = Statistics.newEmptyStats(dataType);
        result.insertAll(datapoints.subMap(startTime, true, endTime, true));
        return result;
    }

    @Override
    public DataPointList periodQuery(long startTime, long endTime, int limit) throws TimeseriesException {
        if (startTime < this.beginTimestamp)
            throw new TimeseriesException("period query over-headed");

        DataPointList resultList = new DataPointList();

        for(DataPoint dataPoint : datapoints.subMap(startTime, true, endTime, true).values()){
            resultList.dataPointList.add(dataPoint);
            if (--limit == 0) break;
        }

        // find successor leaf for more data
        NestNodeLeaf currentLeaf = this;
        while (limit != 0 && endTime > currentLeaf.datapoints.lastKey() && currentLeaf.succRID.isValid()) {
            // continue on next leaf, load as READ_ONLY
            currentLeaf = (NestNodeLeaf) NestNode.loadNonRoot(currentLeaf.succRID, documentType, degree, dataType, -1, -1, null);

            for(DataPoint dataPoint : currentLeaf.datapoints.subMap(startTime, true, endTime, true).values()){
                resultList.dataPointList.add(dataPoint);
                if (--limit == 0) break;
            }
        }

        return resultList;
    }
}
