package com.arcadedb.timeseries;

import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;

public abstract class ArcadeDocument {
    // manager of this document
    public final ArcadeDocumentManager manager;
    // arcadeDB document
    public final Document document;
    // if this document is dirty
    public boolean dirty = false;
    public boolean cahced = false;

    public ArcadeDocument(ArcadeDocumentManager manager, Document document) {
        this.manager = manager;
        this.document = document;
    }

    // save ArcadeDB document
    public void save(){
        if (document.getIdentity() == null){
            // new document
            try {
                serializeDocument().save();
            } catch (TimeseriesException e) {
                throw new RuntimeException(e);
            }
            manager.putCache(this);
            dirty = false;
        }else if (dirty){
            try {
                serializeDocument().save();
            } catch (TimeseriesException e) {
                throw new RuntimeException(e);
            }
            dirty = false;
        }
    }

    // put data into ArcadeDB document
    public abstract MutableDocument serializeDocument() throws TimeseriesException;

    // set this document to dirty
    public void setAsDirty(){
        // ensure document is in cache
        if (!cahced)
            manager.putCache(this);
        dirty = true;
    }
}
