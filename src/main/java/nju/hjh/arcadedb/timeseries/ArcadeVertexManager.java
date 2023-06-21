package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ArcadeVertexManager {
    // max size of cache
    public static final int MAX_CACHE_SIZE = 1024;
    // initial size of cache
    public static final int INIT_CACHE_SIZE = 64;

    // created instances of ArcadeVertexManager
    public static HashMap<Database, ArcadeVertexManager> managerInstances = new HashMap<>();
    // arcadeDB database
    public Database database;
    // null RID
    public RID nullRID;
    // LRU cache
    public LinkedHashMap<RID, ArcadeVertex> cache = new LinkedHashMap<>(INIT_CACHE_SIZE, 0.75f, true){
        @Override
        protected boolean removeEldestEntry(Map.Entry<RID, ArcadeVertex> eldest) {
            if (size() > MAX_CACHE_SIZE){
                ArcadeVertex eldestVertex = eldest.getValue();
                eldestVertex.cahced = false;
                eldestVertex.save();
                return true;
            }
            return false;
        }
    };

    public ArcadeVertexManager(Database database){
        this.database = database;
        nullRID = new RID(database, -1, -1);
    }

    public static ArcadeVertexManager getInstance(Database database){
        ArcadeVertexManager manager = managerInstances.get(database);
        if (manager != null) return manager;
        manager = new ArcadeVertexManager(database);
        managerInstances.put(database, manager);
        return manager;
    }

    public RID getRID(int bucketId, long offset){
        return new RID(database, bucketId, offset);
    }

    /** create new ArcadeVertex,
     * this vertex will not be persisted until save() called
     * @param vertexType type name of vertex
     * @param builder a ArcadeVertex builder from a single Vertex
     * @return new ArcadeVertex object
     */
    public ArcadeVertex newArcadeVertex(String vertexType, ArcadeVertexBuilder builder) throws TimeseriesException {
        ArcadeVertex newVertex = builder.build(database.newVertex(vertexType));

        return newVertex;
    }

    // get existing vertex from arcadeDB
    public ArcadeVertex getArcadeVertex(RID rid, ArcadeVertexBuilder builder) throws TimeseriesException {
        ArcadeVertex result;
        if ((result = cache.get(rid)) != null){
            return result;
        }
        result = builder.build(database.lookupByRID(rid, true).asVertex());

        putCache(result);

        return result;
    }

    public void putCache(ArcadeVertex vertex){
        RID vertexRID = vertex.vertex.getIdentity();
        if (vertexRID != null){
            cache.put(vertexRID, vertex);
            vertex.cahced = true;
        }
    }

    // save all dirty vertexes
    public void saveAll(){
        for (ArcadeVertex vertex : cache.values()){
            vertex.save();
        }
    }

    public void clearCache(){
        cache.clear();
    }
}
