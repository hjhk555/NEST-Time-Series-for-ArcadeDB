package nju.hjh.arcadedb.timeseries;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import nju.hjh.arcadedb.timeseries.exception.TimeseriesException;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ArcadeDocumentManager {
    // created instances of ArcadeDocumentManager
    public static final HashMap<Database, ArcadeDocumentManager> managerInstances = new HashMap<>();
    // arcadeDB database
    public Database database;
    // null RID
    public RID nullRID;
    // LRU cache
    public Map<RID, ArcadeDocument> cache = new HashMap<>();

    public ArcadeDocumentManager(Database database){
        this.database = database;
        nullRID = new RID(database, -1, -1);
    }

    public static ArcadeDocumentManager getInstance(Database database){
        synchronized (managerInstances) {
            ArcadeDocumentManager manager = managerInstances.get(database);
            if (manager != null) {
                // check if manager has same database
                if (manager.database == database) return manager;
            }
            manager = new ArcadeDocumentManager(database);
            managerInstances.put(database, manager);
            return manager;
        }
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

    public void putCache(ArcadeDocument document) throws TimeseriesException {
        RID documentRID = document.document.getIdentity();
        if (documentRID != null)
            cache.put(documentRID, document);
        else
            throw new TimeseriesException("cannot push document without rid into cache");
    }

    // save all dirty documents
    public void saveAll() throws TimeseriesException {
        for (ArcadeDocument document : cache.values()){
            document.save();
        }
    }

    public void clearCache(){
        cache.clear();
    }
}
