package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ArcadeDocumentManager {
    // max size of cache
    public static final int MAX_CACHE_SIZE = 1024;
    // initial size of cache
    public static final int INIT_CACHE_SIZE = 64;

    // created instances of ArcadeDocumentManager
    public static HashMap<Database, ArcadeDocumentManager> managerInstances = new HashMap<>();
    // arcadeDB database
    public Database database;
    // null RID
    public RID nullRID;
    // LRU cache
    public LinkedHashMap<RID, ArcadeDocument> cache = new LinkedHashMap<>(INIT_CACHE_SIZE, 0.75f, true){
        @Override
        protected boolean removeEldestEntry(Map.Entry<RID, ArcadeDocument> eldest) {
            if (size() > MAX_CACHE_SIZE){
                ArcadeDocument eldestDocument = eldest.getValue();
                eldestDocument.cahced = false;
                eldestDocument.save();
                return true;
            }
            return false;
        }
    };

    public ArcadeDocumentManager(Database database){
        this.database = database;
        nullRID = new RID(database, -1, -1);
    }

    public static ArcadeDocumentManager getInstance(Database database){
        ArcadeDocumentManager manager = managerInstances.get(database);
        if (manager != null) return manager;
        manager = new ArcadeDocumentManager(database);
        managerInstances.put(database, manager);
        return manager;
    }

    public RID getRID(int bucketId, long offset){
        return new RID(database, bucketId, offset);
    }

    /** create new ArcadeDocument,
     * this document will not be persisted until save() called
     * @param documentType type name of document
     * @param builder a ArcadeDocument builder from a single Document
     * @return new ArcadeDocument object
     */
    public ArcadeDocument newArcadeDocument(String documentType, ArcadeDocumentBuilder builder) throws TimeseriesException {
        ArcadeDocument newDocument = builder.build(database.newDocument(documentType));

        return newDocument;
    }

    // get existing document from arcadeDB
    public ArcadeDocument getArcadeDocument(RID rid, ArcadeDocumentBuilder builder) throws TimeseriesException {
        ArcadeDocument result;
        if ((result = cache.get(rid)) != null){
            return result;
        }
        result = builder.build(database.lookupByRID(rid, true).asDocument());

        putCache(result);

        return result;
    }

    public void putCache(ArcadeDocument document){
        RID documentRID = document.document.getIdentity();
        if (documentRID != null){
            cache.put(documentRID, document);
            document.cahced = true;
        }
    }

    // save all dirty documents
    public void saveAll(){
        for (ArcadeDocument document : cache.values()){
            document.save();
        }
    }

    public void clearCache(){
        cache.clear();
    }
}
