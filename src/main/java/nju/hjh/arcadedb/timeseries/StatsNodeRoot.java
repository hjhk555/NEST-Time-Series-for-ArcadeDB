package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;

import java.util.ArrayList;

public class StatsNodeRoot extends StatsNode {
    public static final byte BLOCK_TYPE = 0;

    /**
     * size of stat header without statistics and child list:
     * block type(1B) + degree(4B) + data type(5B) + latestRID(12B) + latestStartTime(8B) + child size(4B)
     */
    public static final int HEADER_WITHOUT_STATS_AND_CHILD = 34;

    public RID latestRID;
    public long latestStartTime;
    public ArrayList<RID> childRID = new ArrayList<>();
    public ArrayList<Long> childStartTime = new ArrayList<>();

    public StatsNodeRoot(ArcadeDocumentManager manager, Document document, String metric, int degree, DataType dataType) {
        super(manager, document, metric, degree, dataType);
        // root is always latest and start at 0
        setStartTime(0);
        setActive(true);
    }

    @Override
    public void setParent(StatsNode parent) {
        // ignore
    }

    @Override
    public MutableDocument serializeDocument() throws TimeseriesException {
        int statSize = HEADER_WITHOUT_STATS_AND_CHILD + Statistics.maxBytesRequired(dataType) + degree * CHILD_SIZE;

        MutableDocument modifiedDocument = document.modify();
        Binary binary = new Binary(statSize, false);
        binary.putByte(BLOCK_TYPE);
        binary.putInt(degree);
        dataType.serialize(binary);
        binary.putInt(latestRID.getBucketId());
        binary.putLong(latestRID.getPosition());
        binary.putLong(latestStartTime);
        statistics.serialize(binary);
        binary.putInt(childRID.size());
        for (int i=0; i<childRID.size(); i++){
            binary.putInt(childRID.get(i).getBucketId());
            binary.putLong(childRID.get(i).getPosition());
            binary.putLong(childStartTime.get(i));
        }

        if (binary.size() > statSize)
            throw new TimeseriesException("stat header size exceeded");

        binary.size(statSize);
        modifiedDocument.set(PROP_NODE_INFO, binary.toByteArray());

        return modifiedDocument;
    }


    @Override
    public void insert(DataPoint data, UpdateStrategy strategy) throws TimeseriesException {
        if (data.timestamp >= latestStartTime)
            insertLatest(data, strategy);
        else
            insertTree(data, strategy);
    }

    private void insertLatest(DataPoint data, UpdateStrategy strategy) throws TimeseriesException{
        StatsNodeLeaf latestBlock = (StatsNodeLeaf) getStatsBlockNonRoot(manager, latestRID, metric, degree, dataType);
        latestBlock.setStartTime(latestStartTime);
        latestBlock.setActive(true);
        latestBlock.insert(data, strategy);

        // check if latest block split
        while (latestBlock.succRID.isValid()){
            addLeafBlock(latestBlock);
            latestRID = latestBlock.succRID;
            latestBlock = (StatsNodeLeaf) StatsNode.getStatsBlockNonRoot(manager, latestRID, metric, degree, dataType);
            latestStartTime = latestBlock.startTime;
            setAsDirty();
        }
    }

    private void insertTree(DataPoint data, UpdateStrategy strategy) throws TimeseriesException {
        if (childStartTime.size() == 0)
            throw new TimeseriesException("cannot insert datapoint into tree as there's no child block");

        int insertPos = MathUtils.longBinarySearchFormer(childStartTime, data.timestamp);
        if (insertPos == -1)
            throw new TimeseriesException("cannot insert datapoint into tree as no child block can manage target timestamp");

        StatsNode blockToInsert = getStatsBlockNonRoot(manager, childRID.get(insertPos), metric, degree, dataType);
        blockToInsert.setParent(this);
        blockToInsert.setStartTime(this.childStartTime.get(insertPos));
        blockToInsert.setActive(blockToInsert instanceof StatsNodeInternal && (insertPos == childRID.size()-1));
        blockToInsert.insert(data, strategy);
    }

    @Override
    public void appendStats(DataPoint data) throws TimeseriesException {
        this.statistics.insert(data);
        setAsDirty();
    }

    @Override
    public void updateStats(DataPoint oldDP, DataPoint newDP) throws TimeseriesException {
        if (!this.statistics.update(oldDP, newDP)){
            this.statistics = Statistics.newEmptyStats(dataType);
            for (RID rid : childRID) {
                this.statistics.merge(getStatsBlockNonRoot(manager, rid, metric, degree, dataType).statistics);
            }
        }
        setAsDirty();
    }

    @Override
    public void addChild(StatsNode child) throws TimeseriesException {
        int insertPos = MathUtils.longBinarySearchLatter(childStartTime, child.startTime);
        if (insertPos < childStartTime.size() && childStartTime.get(insertPos) == child.startTime)
            throw new TimeseriesException("cannot insert child with existing startTime");

        childRID.add(insertPos, child.document.getIdentity());
        childStartTime.add(insertPos, child.startTime);

        if (childRID.size() > degree){
            // create two internal block to hold root's data
            StatsNodeInternal newInternalFormer = (StatsNodeInternal) manager.newArcadeDocument(PREFIX_STATS_NODE +metric, document1 -> {
                return new StatsNodeInternal(manager, document1, metric, degree, dataType);
            });
            StatsNodeInternal newInternalLatter = (StatsNodeInternal) manager.newArcadeDocument(PREFIX_STATS_NODE +metric, document1 -> {
                return new StatsNodeInternal(manager, document1, metric, degree, dataType);
            });

            // former data transfer
            newInternalFormer.childRID = new ArrayList<>(this.childRID.subList(0, degree));
            newInternalFormer.childStartTime = new ArrayList<>(this.childStartTime.subList(0, degree));
            newInternalFormer.statistics = Statistics.newEmptyStats(dataType);
            for (RID rid : newInternalFormer.childRID)
                newInternalFormer.statistics.merge(StatsNode.getStatsBlockNonRoot(manager, rid, metric, degree, dataType).statistics);
            newInternalFormer.save();

            // latter data transfer
            long latterStartTime = this.childStartTime.get(degree);
            newInternalLatter.childRID = new ArrayList<>(this.childRID.subList(degree, childRID.size()));
            newInternalLatter.childStartTime = new ArrayList<>(this.childStartTime.subList(degree, childRID.size()));
            newInternalLatter.statistics = Statistics.newEmptyStats(dataType);
            for (RID rid : newInternalLatter.childRID)
                newInternalLatter.statistics.merge(StatsNode.getStatsBlockNonRoot(manager, rid, metric, degree, dataType).statistics);
            newInternalLatter.save();

            // remake root's data
            this.childRID = new ArrayList<>();
            this.childStartTime = new ArrayList<>();
            this.childRID.add(newInternalFormer.document.getIdentity());
            this.childRID.add(newInternalLatter.document.getIdentity());
            this.childStartTime.add(0L);
            this.childStartTime.add(latterStartTime);
        }

        setAsDirty();
    }

    @Override
    public void addLeafBlock(StatsNodeLeaf leaf) throws TimeseriesException {
        // merge statistics
        statistics.merge(leaf.statistics);

        // empty tree, insert into root
        if (childRID.size() == 0)
            this.addChild(leaf);
        else {
            // locate insert block
            int insertPos = MathUtils.longBinarySearchFormer(childStartTime, leaf.startTime);
            StatsNode blockToInsert = StatsNode.getStatsBlockNonRoot(manager, childRID.get(insertPos), metric, degree, dataType);
            blockToInsert.setStartTime(childStartTime.get(insertPos));
            blockToInsert.setParent(this);
            blockToInsert.setActive(blockToInsert instanceof StatsNodeInternal && (insertPos == childRID.size() - 1));
            blockToInsert.addLeafBlock(leaf);
        }

        setAsDirty();
    }

    @Override
    public Statistics aggregativeQuery(long startTime, long endTime) throws TimeseriesException {
        // get latest block stats
        Statistics resultStats = StatsNode.getStatsBlockNonRoot(manager, latestRID, metric, degree, dataType).aggregativeQuery(startTime, endTime);

        // if range out of root's statistics
        if (startTime > statistics.lastTime || endTime < statistics.firstTime){
            return resultStats;
        }
        // if range covers root's statistics
        if (startTime <= statistics.firstTime && endTime >= statistics.lastTime){
            resultStats.merge(this.statistics);
            return resultStats;
        }

        // locate first block
        int pos;
        if (childStartTime.get(0) >= startTime)
            // start from head
            pos = 0;
        else
            pos = MathUtils.longBinarySearchFormer(childStartTime, startTime);

        // merge non-latest block's statistics
        while (pos < childRID.size() && childStartTime.get(pos) <= endTime){
            resultStats.merge(getStatsBlockNonRoot(manager, childRID.get(pos), metric, degree, dataType).aggregativeQuery(startTime, endTime));
            pos++;
        }
        return resultStats;
    }

    @Override
    public DataPointList periodQuery(long startTime, long endTime, int limit) throws TimeseriesException {
        // period start from latest block
        if (startTime >= latestStartTime)
            return getStatsBlockNonRoot(manager, latestRID, metric, degree, dataType).periodQuery(startTime, endTime, limit);

        // locate first block
        int startPos = MathUtils.longBinarySearchFormer(childStartTime, startTime);
        return getStatsBlockNonRoot(manager, childRID.get(startPos), metric, degree, dataType).periodQuery(startTime, endTime, limit);
    }
}
