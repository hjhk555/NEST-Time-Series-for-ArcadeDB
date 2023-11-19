package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;

import java.util.ArrayList;

public abstract class StatsNode extends ArcadeDocument{
    public static final int DEFAULT_TREE_DEGREE = 32;
    public static final int MAX_DATA_BLOCK_SIZE = 4096;
    public static final String PREFIX_STATS_NODE = "_s";
    public static final String PREFIX_DATA_NODE = "_d";
    public static final String PROP_NODE_INFO = "inf";
    public static final String PROP_NODE_DATA = "dat";

    /**
     * size of one child block
     * childRID(12B) + childStartTime(8B)
     */
    public static final int CHILD_SIZE = 20;

    public final String metric;
    public final int degree;
    public final DataType dataType;
    public long startTime = 0;
    public Statistics statistics;
    public boolean isActive = false;

    public StatsNode(ArcadeDocumentManager manager, Document document, String metric, int degree, DataType dataType){
        super(manager, document);
        this.metric = metric;
        this.degree = degree;
        this.dataType = dataType;
    }

    public static StatsNodeRoot newStatsTree(ArcadeDocumentManager manager, String metric, DataType dataType) throws TimeseriesException {
        return newStatsTree(manager, metric, dataType, DEFAULT_TREE_DEGREE);
    }

    public static StatsNodeRoot newStatsTree(ArcadeDocumentManager manager, String metric, DataType dataType, int degree) throws TimeseriesException {
        // root node
        StatsNodeRoot newTreeRoot = (StatsNodeRoot) manager.newArcadeDocument(PREFIX_STATS_NODE +metric, document1 -> {
            StatsNodeRoot root = new StatsNodeRoot(manager, document1, metric, degree, dataType);
            root.childRID = new ArrayList<>();
            root.childStartTime = new ArrayList<>();
            root.statistics = Statistics.newEmptyStats(dataType);
            return root;
        });

        // leaf node
        StatsNodeLeaf newLeaf = (StatsNodeLeaf) manager.newArcadeDocument(PREFIX_DATA_NODE +metric, document2 ->{
            StatsNodeLeaf leaf = new StatsNodeLeaf(manager, document2, metric, degree, dataType);
            leaf.dataList = new ArrayList<>();
            leaf.prevRID = manager.nullRID;
            leaf.succRID = manager.nullRID;
            return leaf;
        });

        newLeaf.save();

        // link leaf to root
        newTreeRoot.latestRID = newLeaf.document.getIdentity();
        newTreeRoot.latestStartTime = 0;

        newTreeRoot.save();

        return newTreeRoot;
    }

    public static StatsNodeRoot getStatsBlockRoot(ArcadeDocumentManager manager, RID rid, String metric) throws TimeseriesException {
        return (StatsNodeRoot) manager.getArcadeDocument(rid, document1 -> {
            Binary binary = new Binary(document1.getBinary(PROP_NODE_INFO));
            if (binary.getByte() == StatsNodeRoot.BLOCK_TYPE) {
                int degree = binary.getInt();
                DataType dataType = DataType.resolveFromBinary(binary);
                StatsNodeRoot root = new StatsNodeRoot(manager, document1, metric, degree, dataType);
                root.latestRID = manager.getRID(binary.getInt(), binary.getLong());
                root.latestStartTime = binary.getLong();
                root.statistics = Statistics.getStatisticsFromBinary(dataType, binary);
                int childSize = binary.getInt();
                for (int i = 0; i < childSize; i++) {
                    root.childRID.add(manager.getRID(binary.getInt(), binary.getLong()));
                    root.childStartTime.add(binary.getLong());
                }
                return root;
            }else{
                throw new TimeseriesException("target StatsBlock is not root");
            }
        });
    }

    public static StatsNode getStatsBlockNonRoot(ArcadeDocumentManager manager, RID rid, String metric, int degree, DataType dataType) throws TimeseriesException {
        StatsNode statsNode = (StatsNode) manager.getArcadeDocument(rid, document1 -> {
            Binary binary = new Binary(document1.getBinary(PROP_NODE_INFO));
            switch (binary.getByte()){
                case StatsNodeInternal.BLOCK_TYPE -> {
                    StatsNodeInternal internal = new StatsNodeInternal(manager, document1, metric, degree, dataType);
                    internal.statistics = Statistics.getStatisticsFromBinary(internal.dataType, binary);
                    int childSize = binary.getInt();
                    for (int i=0; i<childSize; i++){
                        internal.childRID.add(manager.getRID(binary.getInt(), binary.getLong()));
                        internal.childStartTime.add(binary.getLong());
                    }
                    return internal;
                }
                case StatsNodeLeaf.BLOCK_TYPE -> {
                    StatsNodeLeaf leaf = new StatsNodeLeaf(manager, document1, metric, degree, dataType);
                    leaf.statistics = Statistics.getStatisticsFromBinary(leaf.dataType, binary);
                    leaf.prevRID = manager.getRID(binary.getInt(), binary.getLong());
                    leaf.succRID = manager.getRID(binary.getInt(), binary.getLong());
                    return leaf;
                }
                case StatsNodeRoot.BLOCK_TYPE -> {
                    throw new TimeseriesException("target StatsBlock is root");
                }
                default -> {
                    throw new TimeseriesException("target StatsBlock has invalid type");
                }
            }
        });
        return statsNode;
    }

    public void setStartTime(long startTime){
        this.startTime = startTime;
    }
    public void setActive(boolean isActive){
        this.isActive = isActive;
    }

    public abstract void setParent(StatsNode parent);

    /**
     * inesrt data point into statsBlock
     * @param data the data point to insert
     * @throws TimeseriesException
     */
    public abstract void insert(DataPoint data, UpdateStrategy strategy) throws TimeseriesException;

    // append statistics of out-of-order data point
    public abstract void appendStats(DataPoint data) throws TimeseriesException;

    // update statistics of updated data point
    public abstract void updateStats(DataPoint oldDP, DataPoint newDP) throws TimeseriesException;

    // add child node without appending statistics
    public abstract void addChild(StatsNode child) throws TimeseriesException;

    // insert leaf block into tree
    public abstract void addLeafBlock(StatsNodeLeaf leaf) throws TimeseriesException;

    public abstract Statistics aggregativeQuery(long startTime, long endTime) throws TimeseriesException;

    public abstract DataPointList periodQuery(long startTime, long endTime, int limit) throws TimeseriesException;
}
