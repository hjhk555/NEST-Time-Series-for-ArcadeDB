package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Database;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import nju.hjh.arcadedb.timeseries.datapoint.DataPoint;
import nju.hjh.arcadedb.timeseries.exception.DuplicateTimestampException;
import nju.hjh.arcadedb.timeseries.exception.TargetNotFoundException;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;
import nju.hjh.arcadedb.timeseries.statistics.Statistics;

import java.util.*;

public class TimeseriesEngine {
    public static final String PREFIX_METRIC_EDGE = "_mtrc_";
    public static final int METRIC_EDGE_BUCKETS = 1;
    public static final int STATSBLOCK_BUCKETS = 1;
    public final Database database;
    public final ArcadeDocumentManager manager;

    // created instances of TimeseriesEngine
    public static HashMap<Database, TimeseriesEngine> engineInstances = new HashMap<>();

    public TimeseriesEngine(Database database) {
        this.database = database;
        this.manager = ArcadeDocumentManager.getInstance(database);
    }

    public static TimeseriesEngine getInstance(Database database){
        TimeseriesEngine engine = engineInstances.get(database);
        if (engine != null) return engine;
        engine = new TimeseriesEngine(database);
        engineInstances.put(database, engine);
        return engine;
    }

    public Set<String> getAllMetrics(Vertex object){
        Set<String> res = new HashSet<>();
        for (Edge edge : object.getEdges(Vertex.DIRECTION.OUT)) {
            String edgeTypeName = edge.getTypeName();
            if (edgeTypeName.startsWith(PREFIX_METRIC_EDGE)) {
                res.add(edgeTypeName.substring(PREFIX_METRIC_EDGE.length()));
            }
        }
        return res;
    }

    public StatsBlockRoot getStatsTreeRoot(Vertex object, String metric) throws TimeseriesException {
        final String metricEdgeType = PREFIX_METRIC_EDGE +metric;
        final String objectMetric = object.getIdentity().getBucketId()+"_"+metric;

        // metric edge not exist
        if (!database.getSchema().existsType(metricEdgeType))
            throw new TargetNotFoundException("metric edgeType "+metricEdgeType+" not exist");

        // object's statsBlock document not exist
        if (!database.getSchema().existsType(StatsBlock.PREFIX_STATSBLOCK+objectMetric))
            throw new TargetNotFoundException("object's statsBlock documentType "+StatsBlock.PREFIX_STATSBLOCK+objectMetric+" not exist");

        Iterator<Edge> edgeIterator = object.getEdges(Vertex.DIRECTION.OUT, metricEdgeType).iterator();

        // no existing statsBlockRoot
        if (!edgeIterator.hasNext()){
            throw new TargetNotFoundException("object has no metric "+metric);
        }else{
            Edge metricedge = edgeIterator.next();
            // have more than 1 statsTree
            if (edgeIterator.hasNext()){
                throw new TimeseriesException("object has more than 1 outEdge to metric "+metric);
            }
            return StatsBlock.getStatsBlockRoot(manager, metricedge.getIn(), objectMetric);
        }
    }

    public StatsBlockRoot getOrNewStatsTreeRoot(Vertex object, String metric, DataType dataType, int statsTreeDegree) throws TimeseriesException {
        final String metricEdgeType = PREFIX_METRIC_EDGE +metric;
        final String objectMetric = object.getIdentity().getBucketId()+"_"+metric;

        // create metric edge if not exist
        if (!database.getSchema().existsType(metricEdgeType))
            database.getSchema().createEdgeType(metricEdgeType, METRIC_EDGE_BUCKETS);

        // create object's metric document if not exist
        if (!database.getSchema().existsType(StatsBlock.PREFIX_STATSBLOCK+objectMetric)) {
            DocumentType statsBlockType = database.getSchema().createDocumentType(StatsBlock.PREFIX_STATSBLOCK + objectMetric, STATSBLOCK_BUCKETS);
            statsBlockType.createProperty("stat", Type.BINARY);
            statsBlockType.createProperty("data", Type.BINARY);
        }

        Iterator<Edge> edgeIterator = object.getEdges(Vertex.DIRECTION.OUT, metricEdgeType).iterator();

        // get root of statsTree
        StatsBlockRoot treeRoot;
        // no existing statsBlockRoot, create one
        if (!edgeIterator.hasNext()){
            treeRoot = StatsBlock.newStatsTree(manager, objectMetric, dataType, statsTreeDegree);
            // link edge
            object.newEdge(metricEdgeType, treeRoot.document, false).save();
        }else{
            Edge metricedge = edgeIterator.next();
            // have more than 1 statsTree
            if (edgeIterator.hasNext()){
                throw new TimeseriesException("object has out edge to more than 1 target metric "+ metric);
            }
            treeRoot = StatsBlock.getStatsBlockRoot(manager, metricedge.getIn(), objectMetric);
        }

        return treeRoot;
    }

    /**
     *
     * @param object object to insert data point
     * @param metric name of metric
     * @param dataType type of data
     * @param dataPoint data point to insert
     * @param strategy update strategy if data point exist at target timestamp
     * @throws DuplicateTimestampException if <code>strategy</code> is ERROR and data point already exist at target timestamp
     */
    public void insertDataPoint(Vertex object, String metric, DataType dataType, DataPoint dataPoint, TSUpdateStrategy strategy) throws TimeseriesException {
        insertDataPoint(object, metric, dataType, dataPoint, strategy, StatsBlock.DEFAULT_TREE_DEGREE);
    }

    public void insertDataPoint(Vertex object, String metric, DataType dataType, DataPoint dataPoint, TSUpdateStrategy strategy, int statsTreeDegree) throws TimeseriesException {
        StatsBlockRoot root = getOrNewStatsTreeRoot(object, metric, dataType, statsTreeDegree);
        dataPoint = root.dataType.checkAndConvertDataPoint(dataPoint);
        root.insert(dataPoint, strategy);
    }

    public Statistics aggregativeQuery(Vertex object, String metric, long startTime, long endTime) throws TimeseriesException {
        return getStatsTreeRoot(object, metric).aggregativeQuery(startTime, endTime);
    }

    public DataPointSet periodQuery(Vertex object, String metric, long startTime, long endTime) throws TimeseriesException {
        return getStatsTreeRoot(object, metric).periodQuery(startTime, endTime);
    }

    public void begin(){
        database.begin();
    }

    public void commit(){
        manager.saveAll();
        database.commit();
    }

    public void rollback(){
        manager.clearCache();
        database.rollback();
    }

}
