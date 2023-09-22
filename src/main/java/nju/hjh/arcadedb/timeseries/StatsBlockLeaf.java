package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;

import java.util.ArrayList;

public class StatsBlockLeaf extends StatsBlock{
    public static final byte BLOCK_TYPE = 2;

    /** size of stat header without statistics:
     *  block type(1B) + prevRID(12B) + nextRID(12B)
     */
    public static final int HEADER_WITHOUT_STATS = 25;

    public StatsBlock parent;
    public ArrayList<DataPoint> dataList;
    public RID prevRID;
    public RID succRID;
    // for unfixed data type
    public int dataBytesUseed;

    public StatsBlockLeaf(ArcadeDocumentManager manager, Document document, String metric, int degree, DataType dataType) throws TimeseriesException {
        super(manager, document, metric, degree, dataType);
        statistics = Statistics.newEmptyStats(dataType);
    }

    void loadData() throws TimeseriesException {
        if (dataList == null) {
            dataList = new ArrayList<>();
            Binary binary = new Binary(document.getBinary("data"));
            for (int i = 0; i < statistics.count; i++)
                dataList.add(DataPoint.getDataPointFromBinary(dataType, binary));
            if (!dataType.isFixed()){
                dataBytesUseed = 0;
                for (DataPoint dataPoint : dataList)
                    dataBytesUseed += dataPoint.realBytesRequired();
            }
        }
    }

    @Override
    public MutableDocument serializeDocument() throws TimeseriesException {
        int statSize = HEADER_WITHOUT_STATS + Statistics.maxBytesRequired(dataType);

        MutableDocument mutableDocument = document.modify();
        // put stat
        Binary statBinary = new Binary(statSize, false);
        statBinary.putByte(BLOCK_TYPE);
        statistics.serialize(statBinary);
        statBinary.putInt(prevRID.getBucketId());
        statBinary.putLong(prevRID.getPosition());
        statBinary.putInt(succRID.getBucketId());
        statBinary.putLong(succRID.getPosition());

        if (statBinary.size() > statSize)
            throw new TimeseriesException("stat header size exceeded");

        statBinary.size(statSize);
        mutableDocument.set("stat", statBinary.toByteArray());

        // put data
        if (dataList != null){
            Binary dataBinary = new Binary(MAX_DATA_BLOCK_SIZE, false);
            for (int i=0; i<statistics.count; i++){
                dataList.get(i).serialize(dataBinary);
            }

            if (dataBinary.size() > MAX_DATA_BLOCK_SIZE)
                throw new TimeseriesException("data block size exceeded");

            dataBinary.size(MAX_DATA_BLOCK_SIZE);
            mutableDocument.set("data", dataBinary.toByteArray());
        }
        return mutableDocument;
    }

    @Override
    public void setParent(StatsBlock parent) {
        this.parent = parent;
    }

    @Override
    public void insert(DataPoint data, UpdateStrategy strategy) throws TimeseriesException {
        if (data.timestamp < startTime)
            throw new TimeseriesException("target dataPoint should not be handled by this leaf");

        if (dataList == null)
            loadData();

        if (dataType.isFixed())
            insertFixed(data, strategy);
        else{
            insertUnfixed(data, strategy);
        }
    }

    public void insertFixed(DataPoint data, UpdateStrategy strategy) throws TimeseriesException {
        int maxdataSize = MAX_DATA_BLOCK_SIZE / DataPoint.maxBytesRequired(dataType);

        int insertPos = MathUtils.longBinarySearchLatter(dataList, data.timestamp, object -> object.timestamp);

        if (insertPos < dataList.size() && dataList.get(insertPos).timestamp == data.timestamp){
            // already exist
            DataPoint oldDP = dataList.get(insertPos);
            DataPoint newDP = oldDP.getUpdatedDataPoint(data, strategy);
            if (newDP == null)
                return; // no need to update
            newDP = dataType.checkAndConvertDataPoint(newDP);
            if (oldDP.getValue().equals(newDP.getValue()))
                return; // no need to update
            dataList.set(insertPos, newDP);
            if (!statistics.update(oldDP, newDP)){
                statistics = Statistics.countStats(dataType, dataList, true);
            }
            if (!isActive)
                parent.updateStats(oldDP, newDP);
        }else {
            dataList.add(insertPos, data);
            statistics.insert(data);
            if (!isActive)
                parent.appendStats(data);
        }

        // split if full
        if (dataList.size() > maxdataSize){
            // split into 2 blocks
            int totalSize = dataList.size();
            int splitedSize;
            if (isActive)
                splitedSize = maxdataSize;
            else
                splitedSize = totalSize / 2;

            StatsBlockLeaf newLeaf = (StatsBlockLeaf) manager.newArcadeDocument(PREFIX_STATSBLOCK+ metric, document1 -> {
                return new StatsBlockLeaf(manager, document1, metric, degree, dataType);
            });
            newLeaf.setStartTime(this.dataList.get(splitedSize).timestamp);
            newLeaf.dataList = new ArrayList<>(this.dataList.subList(splitedSize, totalSize));

            // calc latter half statistics
            newLeaf.statistics = Statistics.countStats(dataType, newLeaf.dataList, true);

            // update this block statistics
            this.dataList = new ArrayList<>(this.dataList.subList(0, splitedSize));
            this.statistics = Statistics.countStats(dataType, this.dataList, true);

            // link leaves
            if (isActive){
                // wait root node to handle
                newLeaf.setActive(true);
                this.setActive(false);

                newLeaf.prevRID = this.document.getIdentity();
                newLeaf.succRID = manager.nullRID;
                newLeaf.save();
                this.succRID = newLeaf.document.getIdentity();
            }else {
                StatsBlockLeaf succLeaf = (StatsBlockLeaf) getStatsBlockNonRoot(manager, this.succRID, metric, degree, dataType);
                newLeaf.succRID = this.succRID;
                newLeaf.prevRID = this.document.getIdentity();
                newLeaf.save();
                this.succRID = newLeaf.document.getIdentity();
                succLeaf.prevRID = newLeaf.document.getIdentity();
                succLeaf.setAsDirty();

                parent.addChild(newLeaf);
            }
        }

        setAsDirty();
    }

    private void insertUnfixed(DataPoint data, UpdateStrategy strategy) throws TimeseriesException {
        int insertPos = MathUtils.longBinarySearchLatter(dataList, data.timestamp, object -> object.timestamp);

        if (insertPos < dataList.size() && dataList.get(insertPos).timestamp == data.timestamp){
            // already exsit
            DataPoint oldDP = dataList.get(insertPos);
            DataPoint newDP = oldDP.getUpdatedDataPoint(data, strategy);
            if (newDP == null)
                return; // no need to update
            newDP = dataType.checkAndConvertDataPoint(newDP);
            if (oldDP.getValue().equals(newDP.getValue()))
                return; // no need to update
            dataList.set(insertPos, newDP);
            dataBytesUseed += newDP.realBytesRequired() - oldDP.realBytesRequired();
            if (!statistics.update(oldDP, newDP)){
                statistics = Statistics.countStats(dataType, dataList, true);
            }
            if (!isActive)
                parent.updateStats(oldDP, newDP);
        }else {
            dataList.add(insertPos, data);
            dataBytesUseed += data.realBytesRequired();
            statistics.insert(data);
            if (!isActive)
                parent.appendStats(data);
        }

        // check if require split, to achieve split, each data point should use at most MAX_DATA_BLOCK_SIZE/2 bytes
        if (dataBytesUseed > MAX_DATA_BLOCK_SIZE){
            // locate split size
            int splitedBytes, splitedSize;
            if (isActive){
                // fill this block as much as possible
                splitedBytes = dataBytesUseed;
                splitedSize = dataList.size();
                while (splitedBytes > MAX_DATA_BLOCK_SIZE){
                    splitedSize--;
                    splitedBytes -= dataList.get(splitedSize).realBytesRequired();
                }
            }else{
                // try to split evenly.
                splitedBytes = 0;
                splitedSize = 0;
                int halfBytes = dataBytesUseed / 2;

                while (splitedBytes < halfBytes){
                    splitedBytes += dataList.get(splitedSize).realBytesRequired();
                    splitedSize++;
                }

                // one step back if better
                int stepBackBytes = splitedBytes - dataList.get(splitedSize - 1).realBytesRequired();
                if (splitedBytes > MAX_DATA_BLOCK_SIZE || splitedBytes - halfBytes > halfBytes - stepBackBytes){
                    splitedBytes = stepBackBytes;
                    splitedSize--;
                }
            }

            // create latter half leaf node
            StatsBlockLeaf newLeaf = (StatsBlockLeaf) manager.newArcadeDocument(PREFIX_STATSBLOCK+ metric, document1 -> {
                return new StatsBlockLeaf(manager, document1, metric, degree, dataType);
            });
            newLeaf.setStartTime(dataList.get(splitedSize).timestamp);
            newLeaf.dataList = new ArrayList<>(this.dataList.subList(splitedSize, dataList.size()));
            newLeaf.dataBytesUseed = this.dataBytesUseed - splitedBytes;
            newLeaf.statistics = Statistics.countStats(dataType, newLeaf.dataList, true);

            // re-calc stats
            this.dataBytesUseed = splitedBytes;
            this.dataList = new ArrayList<>(this.dataList.subList(0, splitedSize));
            statistics = Statistics.countStats(dataType, dataList, true);

            // link leaves
            if (isActive) {
                // wait root node to handle
                newLeaf.setActive(true);
                this.setActive(false);

                newLeaf.prevRID = this.document.getIdentity();
                newLeaf.succRID = manager.nullRID;
                newLeaf.save();
                this.succRID = newLeaf.document.getIdentity();
            }else{
                StatsBlockLeaf succLeaf = (StatsBlockLeaf) getStatsBlockNonRoot(manager, this.succRID, metric, degree, dataType);
                newLeaf.succRID = this.succRID;
                newLeaf.prevRID = this.document.getIdentity();
                newLeaf.save();
                this.succRID = newLeaf.document.getIdentity();
                succLeaf.prevRID = newLeaf.document.getIdentity();
                succLeaf.setAsDirty();

                parent.addChild(newLeaf);
            }
        }
        setAsDirty();
    }

    @Override
    public void appendStats(DataPoint data) throws TimeseriesException {
        throw new TimeseriesException("leaf node should not append statistics");
    }

    @Override
    public void updateStats(DataPoint oldDP, DataPoint newDP) throws TimeseriesException {
        throw new TimeseriesException("leaf node should not update statistics");
    }

    @Override
    public void addChild(StatsBlock child) throws TimeseriesException {
        throw new TimeseriesException("cannot add child to leaf node");
    }

    @Override
    public void addLeafBlock(StatsBlockLeaf leaf) throws TimeseriesException {
        parent.addChild(leaf);
    }

    @Override
    public Statistics aggregativeQuery(long startTime, long endTime) throws TimeseriesException {
        // if range out of this block
        if (startTime > statistics.lastTime || endTime < statistics.firstTime){
            return Statistics.newEmptyStats(dataType);
        }

        // if range covers this block
        if (startTime <= statistics.firstTime && endTime >= statistics.lastTime){
            return this.statistics.clone();
        }

        if (dataList == null)
            loadData();

        // locate first DataPoint
        int startPos;
        if (statistics.firstTime >= startTime){
            // start from head
            startPos = 0;
        }else {
            startPos = MathUtils.longBinarySearchLatter(dataList, startTime, object -> object.timestamp);
        }

        // locate last DataPoint
        int endPos;
        if (statistics.lastTime <= endTime){
            // end at tail
            endPos = dataList.size()-1;
        }else{
            endPos = MathUtils.longBinarySearchFormer(dataList, endTime, object -> object.timestamp);
        }

        return Statistics.countStats(dataType, dataList.subList(startPos, endPos+1), true);
    }

    @Override
    public DataPointSet periodQuery(long startTime, long endTime) throws TimeseriesException {
        if (startTime < this.startTime)
            throw new TimeseriesException("period query over-headed");

        return new DataPointSet(startTime, endTime, this.document.getIdentity(), manager, metric, degree, dataType);
    }
}
