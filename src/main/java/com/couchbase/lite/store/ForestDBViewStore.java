package com.couchbase.lite.store;

import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Emitter;
import com.couchbase.lite.Manager;
import com.couchbase.lite.Mapper;
import com.couchbase.lite.Misc;
import com.couchbase.lite.Predicate;
import com.couchbase.lite.QueryOptions;
import com.couchbase.lite.QueryRow;
import com.couchbase.lite.Reducer;
import com.couchbase.lite.SpecialKey;
import com.couchbase.lite.Status;
import com.couchbase.lite.View;
import com.couchbase.lite.cbforest.Collatable;
import com.couchbase.lite.cbforest.CollatableReader;
import com.couchbase.lite.cbforest.Config;
import com.couchbase.lite.cbforest.Database;
import com.couchbase.lite.cbforest.DocEnumerator;
import com.couchbase.lite.cbforest.Document;
import com.couchbase.lite.cbforest.EmitFn;
import com.couchbase.lite.cbforest.IndexEnumerator;
import com.couchbase.lite.cbforest.KeyRange;
import com.couchbase.lite.cbforest.KeyStore;
import com.couchbase.lite.cbforest.MapFn;
import com.couchbase.lite.cbforest.MapReduceIndex;
import com.couchbase.lite.cbforest.MapReduceIndexer;
import com.couchbase.lite.cbforest.Mappable;
import com.couchbase.lite.cbforest.OpenFlags;
import com.couchbase.lite.cbforest.Revision;
import com.couchbase.lite.cbforest.Slice;
import com.couchbase.lite.cbforest.Transaction;
import com.couchbase.lite.cbforest.VectorKeyRange;
import com.couchbase.lite.cbforest.VectorMapReduceIndex;
import com.couchbase.lite.cbforest.VersionedDocument;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.support.FileDirUtils;
import com.couchbase.lite.util.Log;

import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hideki on 8/13/15.
 */
public class ForestDBViewStore  implements ViewStore, QueryRowStore{
    public static String TAG = Log.TAG_VIEW;

    public static final String  kViewIndexPathExtension = "viewindex";
    // Size of ForestDB buffer cache allocated for a database
    private static final BigInteger kDBBufferCacheSize = new BigInteger("8388608");

    // ForestDB Write-Ahead Log size (# of records)
    private static final BigInteger kDBWALThreshold = new BigInteger("1024");

    // Close the index db after it's inactive this many seconds
    private static final Float kCloseDelay = 60.0f;

    private static final int REDUCE_BATCH_SIZE = 100;

    /**
     * in CBLView.h
     * enum CBLViewIndexType
     */
    public enum CBLViewIndexType {
        kUnknown(-1),
        kCBLMapReduceIndex(1), //  < Regular map/reduce _index with JSON keys.
        kCBLFullTextIndex(2),  // < Keys must be strings and will be indexed by the words they contain.
        kCBLGeoIndex(3);       // < Geo-query _index; not supported yet.
        private int value;
        CBLViewIndexType(int value) {
            this.value = value;
        }
        public int getValue() {
            return value;
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // #pragma mark - C++ MAP/REDUCE GLUE:
    ///////////////////////////////////////////////////////////////////////////

    /**
     * in CBL_ForestDBViewStorage.mm
     * class CocoaMappable : public Mappable
     */
    class CocoaMappable extends Mappable {
        Map<String, Object> body = null;
        CocoaMappable(Document doc, Map<String, Object> dict) {
            super(doc);
            body = dict;
            Log.w(TAG, "[CocoaMappable.CocoaMappable(Document, Map<String, Object>)]");
        }
    }

    /**
     *  in CBL_ForestDBViewStorage.mm
     *  class CocoaIndexer : public MapReduceIndexer
     */
    class CocoaIndexer extends MapReduceIndexer {

        private KeyStore sourceStore = null;
        private List<String> docTypes = new ArrayList<String>();

        CocoaIndexer(VectorMapReduceIndex indexes) {
            Log.w(TAG, "[CocoaIndexer.CocoaIndexer(VectorMapReduceIndex)]");
            sourceStore = indexes.get(0).sourceStore();
        }

        void addDocType(String type){
            Log.w(TAG, "[CocoaIndexer.addDocType()]");
            docTypes.add(type);
        }

        void clearDocTypes(){
            Log.w(TAG, "[CocoaIndexer.clearDocTypes()]");
            docTypes.clear();
        }

        /**
         * virtual void addDocument(const Document& cppDoc)
         */
        public void addDocument(Document cppDoc) {
            Log.w(TAG, "[CocoaIndexer.addDocument(Document)]");

            // TODO: check sourceStore and cppDoc

            boolean indexIt = true;
            if((VersionedDocument.flagsOfDocument(cppDoc) & VersionedDocument.kDeleted) != 0)
                indexIt = false;
            else if(new String(cppDoc.getKey().getBuf()).startsWith("_design/"))
                indexIt = false;
            else if (docTypes.size() > 0){
                String docType = new String(VersionedDocument.docTypeOfDocument(cppDoc).getBuf());
                if(!docTypes.contains(docType))
                    indexIt = false;
            }

            if (indexIt) {
                VersionedDocument vdoc = new VersionedDocument(sourceStore, cppDoc);
                Revision node = vdoc.currentRevision();
                Map<String, Object> body = ForestBridge.bodyOfNode(node, vdoc);
                body.put("_local_seq", node.getSequence().longValue());
                if (vdoc.hasConflict()) {
                    // TODO
                    // TODO
                }
                Log.w(TAG, "Mapping %s rev %s", body.get("_id"), body.get("_rev"));
                CocoaMappable mappable = new CocoaMappable(cppDoc, body);
                addMappable(mappable);
                vdoc.delete();
            } else {
                CocoaMappable mappable = new CocoaMappable(cppDoc, null);
                addMappable(mappable);
            }
            Log.w(TAG, "[CocoaIndexer.addDocument(Document)] END");
        }
    }

    /**
     * in CBL_ForestDBViewStorage.mm
     * class MapReduceBridge : public MapFn
     */
    public static class MapReduceBridge extends MapFn {
        Mapper mapBlock = null;
        String viewName = null;
        String documentType = null;
        CBLViewIndexType indexType;

        /**
         * virtual void operator() (const Mappable& mappable, EmitFn& emitFn)
         */
        public void call(Mappable mappable, final EmitFn emitFn) {
            Log.w(TAG, "[MapReduceBridge.call()] START");

            CocoaMappable cocoaMappable = (CocoaMappable) mappable;
            final Map<String, Object> doc = cocoaMappable.body;
            if (doc == null)
                return; // doc is deleted or otherwise not to be indexed
            if (documentType != null && !documentType.equals(doc.get("type")))
                return;

            Emitter emitter = new Emitter() {
                public void emit(Object key, Object value) {
                    if (indexType == CBLViewIndexType.kCBLFullTextIndex) {
                        Log.e(TAG, "Full Text Index is not supported yet!!!!");
                    } else if (key instanceof SpecialKey) {
                        Log.e(TAG, "Special Key is not supported yet!!!!");
                    } else if (key != null) {
                        Log.w(TAG, "    emit(%s, %s)  to %s", key, value, viewName);
                        callEmit(key, value, doc, emitFn);
                    }
                }
            };

            if (mapBlock != null)
                mapBlock.map(doc, emitter); // Call the apps' map block!

            Log.w(TAG, "[MapReduceBridge.call()] END");
        }

        // Emit a regular key/value pair
        void callEmit(Object key, Object value, Map<String, Object> doc, EmitFn emitFn) {
            Log.w(TAG, "[MapReduceBridge.callEmit()] START");
            Collatable collKey = new Collatable();
            ForestDBCollatorUtil.add(collKey, key);
            Collatable collValue = new Collatable();
            if (value == doc)
                collValue.addSpecial();// placeholder for doc
            else if (value != null)
                ForestDBCollatorUtil.add(collValue, value);
            emitFn.call(collKey, collValue);
            Log.w(TAG, "[MapReduceBridge.callEmit()] END");
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // ForestDBViewStore
    ///////////////////////////////////////////////////////////////////////////


    // public
    private String name;
    private ViewStoreDelegate delegate;

    // private
    private ForestDBStore _dbStore;
    private String _path;
    Database _indexDB;
    MapReduceIndex _index;
    private CBLViewIndexType _indexType;
    private MapReduceBridge _mapReduceBridge = new MapReduceBridge();


    ///////////////////////////////////////////////////////////////////////////
    // Constructor
    ///////////////////////////////////////////////////////////////////////////

    protected ForestDBViewStore(ForestDBStore dbStore, String name, boolean create) throws CouchbaseLiteException{
        this._dbStore = dbStore;
        this.name = name;
        this._path = new File(dbStore.directory, viewNameToFileName(name)).getPath();
        Log.w(TAG, "_path => " + _path);
        this._indexType = CBLViewIndexType.kUnknown;
        File file = new File(this._path);
        if(!file.exists()){
            if(!create)
                throw new CouchbaseLiteException(Status.NOT_FOUND);
            if(openIndex(OpenFlags.FDB_OPEN_FLAG_CREATE) == null)
                throw new CouchbaseLiteException(Status.UNKNOWN);

        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Implementation of ViewStorage
    ///////////////////////////////////////////////////////////////////////////

    @Override
    public boolean rowValueIsEntireDoc(byte[] valueData) {
        Log.e(TAG, "rowValueIsEntireDoc()");
        return false;
    }

    @Override
    public Object parseRowValue(byte[] valueData) {
        Log.e(TAG, "parseRowValue()");
        return null;
    }

    @Override
    public Map<String, Object> getDocumentProperties(String docID, long sequence) {
        Log.e(TAG, "getDocumentProperties()");
        return null;
    }

    @Override
    public String getName() {
        Log.w(TAG, "getName()");
        return name;
    }

    @Override
    public ViewStoreDelegate getDelegate() {
        Log.w(TAG, "getDelegate()");
        return delegate;
    }

    @Override
    public void setDelegate(ViewStoreDelegate delegate) {
        Log.w(TAG, "setDelegate()");
        this.delegate = delegate;

    }

    @Override
    public void close() {
        Log.w(TAG, "close()");
        closeIndex();

    }

    @Override
    public void deleteIndex() {
        Log.e(TAG, "deleteIndex()");

    }

    @Override
    public void deleteView() {
        Log.w(TAG, "deleteView()");
        closeIndex();
        FileDirUtils.deleteRecursive(new File(_path));
    }

    @Override
    public boolean setVersion(String version) {
        Log.w(TAG, "setVersion() version=" + version);
        return true;
    }

    @Override
    public int getTotalRows() {
        Log.w(TAG, "[getTotalRows()]");
        if(openIndex()==null)
            return 0;
        return _index.rowCount().intValue();
    }

    @Override
    public long getLastSequenceIndexed() {
        Log.w(TAG, "[getLastSequenceIndexed()]");

        if(setupIndex() == null)// in case the _mapVersion changed, invalidating the _index
            return -1;

        long lastSequence = _index.lastSequenceIndexed().longValue();
        return lastSequence;
    }

    @Override
    public long getLastSequenceChangedAt() {
        Log.e(TAG, "getLastSequenceChangedAt()");
        return 0;
    }

    @Override
    public void updateIndex() throws CouchbaseLiteException {
        Log.w(TAG, "[updateIndex()] START");
        updateIndex(Arrays.asList(this));
    }

    private int updateIndex(List<ForestDBViewStore> views) throws CouchbaseLiteException {
        Log.w(TAG, "[updateIndex(List<ForestDBViewStore>)] Checking indexes of (%s) for %s", viewNames(views), name);

        List<Transaction> transactions = new ArrayList<Transaction>();
        CocoaIndexer indexer = null;
        try {
            VectorMapReduceIndex indexes = new VectorMapReduceIndex();
            indexes.add(_index);
            indexer = new CocoaIndexer(indexes);
            indexer.triggerOnIndex(_index);

            boolean useDocTypes = true;
            for (ForestDBViewStore viewStore : views) {
                MapReduceIndex index = viewStore.setupIndex();
                ViewStoreDelegate viewStoreDelegate = viewStore.delegate;
                if (viewStoreDelegate.getMap() == null) {
                    Log.v(TAG, "    %s has no map block; skipping it", viewStore.getName());
                    continue;
                }
                Transaction t = new Transaction(viewStore._indexDB);
                indexer.addIndex(index, t);
                transactions.add(t);
                if (useDocTypes) {
                    String docType = delegate.getDocumentType();
                    if (docType != null) {
                        indexer.addDocType(docType);
                    } else {
                        indexer.clearDocTypes();
                        useDocTypes = false;
                    }
                }
            }
            if (indexer.run()) {
                Log.v(TAG, "... Finished re-indexing (%s) to TODO", viewNames(views));
                return Status.OK;
            } else {
                Log.v(TAG, "... Nothing to do.");
                return Status.NOT_MODIFIED;
            }
        } finally {
            if(indexer != null) {
                // Without calling indexer.delete(), ~MapReduceIndexer() is not called.
                // ~MapReduceIndexer() calls
                indexer.delete();
                indexer = null;
            }
            for (Transaction t : transactions) {
                t.delete();
            }
        }
    }

    @Override
    public List<QueryRow> regularQuery(QueryOptions options) throws CouchbaseLiteException {
        Log.w(TAG, "[regularQuery()]");

        if(openIndex()==null)
            return null;

        final Predicate<QueryRow> postFilter = options.getPostFilter();
        int limit = options.getLimit();
        int skip = options.getSkip();
        if (postFilter != null) {
            // #574: Custom post-filter means skip/limit apply to the filtered rows, not to the
            // underlying query, so handle them specially:
            options.setLimit(QueryOptions.QUERY_OPTIONS_DEFAULT_LIMIT);
            options.setSkip(0);
        }

        List<QueryRow> rows = new ArrayList<QueryRow>();

        IndexEnumerator e = runForestQuery(options);
        try {
            while (e.next()) {
                RevisionInternal docRevision = null;
                CollatableReader crKey = e.key();
                Object key = ForestDBCollatorUtil.read(e.key());
                Object value = ForestDBCollatorUtil.read(e.value());
                Log.w(TAG, "Tag=" + crKey.peekTag()+ ", key="+key+", value="+value);
                String docID = new String(e.docID().getBuf());
                long sequence = e.sequence().longValue();
                if(options.isIncludeDocs()){
                    String linkedID = null;
                    if(value instanceof Map)
                        linkedID = (String) ((Map) value).get("_id");
                    if (linkedID != null) {
                        // Linked document: http://wiki.apache.org/couchdb/Introduction_to_CouchDB_views#Linked_documents
                        String linkedRev = (String) ((Map) value).get("_rev");
                        docRevision = _dbStore.getDocument(linkedID, linkedRev, true);
                        sequence = docRevision.getSequence();
                    } else {
                        docRevision = _dbStore.getDocument(docID, null, true);
                    }
                }
                Log.w(TAG, "Query %s: Found row with key=%s, value=%s, id=%s",
                        name,
                        key == null ? "" : key,
                        value == null ? "" : value,
                        docID);
                QueryRow row = new QueryRow(docID, sequence,
                        key, value,
                        docRevision, this);

                if (postFilter != null) {
                    if (!postFilter.apply(row)) {
                        continue;
                    }
                }

                rows.add(row);
            }
        }catch(Exception ex){
            Log.e(TAG, "Error in regularQuery()", ex);
            throw new CouchbaseLiteException(Status.UNKNOWN);
        }
        finally {
            e.delete();
        }
        return rows;
    }

    /**
     * Queries the view, with reducing or grouping as per the options.
     * in CBL_ForestDBViewStorage.m
     * - (CBLQueryIteratorBlock) reducedQueryWithOptions: (CBLQueryOptions*)options
     *                                            status: (CBLStatus*)outStatus
     */
    @Override
    public List<QueryRow> reducedQuery(QueryOptions options) throws CouchbaseLiteException {
        Log.e(TAG, "reducedQuery()");

        final Predicate<QueryRow> postFilter = options.getPostFilter();

        final int groupLevel = options.getGroupLevel();
        final boolean group = options.isGroup() || (groupLevel > 0);
        final Reducer reduce = delegate.getReduce();
        if (options.isReduceSpecified()) {
            if (options.isReduce() && reduce == null) {
                Log.w(TAG, String.format(
                        "Cannot use reduce option in view %s which has no reduce block defined",
                        name));
                throw new CouchbaseLiteException(new Status(Status.BAD_PARAM));
            }
        }

        final List<Object> keysToReduce = new ArrayList<Object>(REDUCE_BATCH_SIZE);
        final List<Object> valuesToReduce = new ArrayList<Object>(REDUCE_BATCH_SIZE);
        final Object[] lastKeys = new Object[1];
        lastKeys[0] = null;
        final ForestDBViewStore that = this;
        final List<QueryRow> rows = new ArrayList<QueryRow>();

        if(openIndex() == null)
            return null;

        IndexEnumerator e = runForestQuery(new QueryOptions());
        try {
            while (e.next()) {
                Object keyObject = ForestDBCollatorUtil.read(e.key());
                Object valueObject = ForestDBCollatorUtil.read(e.value());

                if (group && !groupTogether(keyObject, lastKeys[0], groupLevel)) {
                    if (lastKeys[0] != null) {
                        // This pair starts a new group, so reduce & record the last one:
                        Object key = groupKey(lastKeys[0], groupLevel);
                        Object reduced = (reduce != null) ?
                                reduce.reduce(keysToReduce, valuesToReduce, false) :
                                null;
                        QueryRow row = new QueryRow(null, 0, key, reduced, null, that);
                        if (postFilter == null || postFilter.apply(row))
                            rows.add(row);
                        keysToReduce.clear();
                        valuesToReduce.clear();
                    }
                    lastKeys[0] = keyObject;
                }

                keysToReduce.add(keyObject);
                valuesToReduce.add(valueObject);
            }
        } catch (Exception ex) {
            Log.e(TAG, "Error in reducedQuery()", ex);
        } finally {
            e.delete();
        }


        if (keysToReduce != null && keysToReduce.size() > 0) {
            // Finish the last group (or the entire list, if no grouping):
            Object key = group ? groupKey(lastKeys[0], groupLevel) : null;
            Object reduced = (reduce != null) ?
                    reduce.reduce(keysToReduce, valuesToReduce, false) :
                    null;
            Log.v(TAG, String.format("Query %s: Reduced to key=%s, value=%s",
                    name, key, reduced));
            QueryRow row = new QueryRow(null, 0, key, reduced, null, that);
            if (postFilter == null || postFilter.apply(row)) {
                rows.add(row);
            }
        }
        return rows;
    }

    @Override
    public List<Map<String, Object>> dump() {
        Log.w(TAG, "dump()");

        MapReduceIndex index = openIndex();
        if(index == null)
            return null;

        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

        IndexEnumerator e = runForestQuery(new QueryOptions());
        try {
            while (e.next()) {

                Map<String, Object> dict = new HashMap<String, Object>();
                dict.put("key", Manager.getObjectMapper().writeValueAsString(ForestDBCollatorUtil.read(e.key())));
                dict.put("value", Manager.getObjectMapper().writeValueAsString(ForestDBCollatorUtil.read(e.value())));
                dict.put("seq", e.sequence().intValue());
                result.add(dict);
            }
        }catch(Exception ex){
            Log.e(TAG, "Error in dump()",ex);
        }finally {
            e.delete();
        }
        return result;
    }

    @Override
    public void setCollation(View.TDViewCollation collation) {
        Log.w(TAG, "setCollation()");
        Log.w(TAG, "This method should be removed");
    }

    ///////////////////////////////////////////////////////////////////////////
    // Internal (Protected/Private)  Methods
    ///////////////////////////////////////////////////////////////////////////

    // Opens the index. You MUST call this (or a method that calls it) before dereferencing _index.
    private MapReduceIndex openIndex() {
        // TODO: should be 0
        return openIndex(OpenFlags.FDB_OPEN_FLAG_CREATE);
    }

    /**
     * Opens the index, specifying ForestDB database flags
     * in CBLView.m
     * - (MapReduceIndex*) openIndexWithOptions: (Database::openFlags)options
     */
    private MapReduceIndex openIndex(OpenFlags options) {
        if (_index == null) {
            try {
                Config config = Database.defaultConfig();
                config.setFlags(options);
                config.setBuffercacheSize(kDBBufferCacheSize);
                config.setWalThreshold(kDBWALThreshold);
                config.setWalFlushBeforeCommit(true);
                //config.seqtree_opt = false; // indexes don't need by-sequence ordering
                //config.compaction_threshold = 50;

                // TODO encryption

                _indexDB = new Database(_path, config);
                Database db = _dbStore.forest;
                _index = new MapReduceIndex(_indexDB, "index", db);
            } catch (Exception e) {
                Log.e(TAG, "Error in openIndex()", e);
            }
        }
        return _index;
    }

    /**
     * in CBL_ForestDBViewStorage.mm
     * - (void) closeIndex
     */
    private void closeIndex() {
        // TODO
        //NSObject cancelPreviousPerformRequestsWithTarget: self selector: @selector(closeIndex) object: nil];

        if (_indexDB != null) {
            Log.v(TAG, "%s: Closing _index db", this);
            _indexDB.delete();
            _indexDB = null;
        }
        if (_index != null) {
            _index.delete();
            _index = null;
        }
    }

    private MapReduceIndex setupIndex() {
        if (delegate == null)
            return null;

        _mapReduceBridge.mapBlock = delegate.getMap();
        _mapReduceBridge.viewName = name;
        _mapReduceBridge.indexType = _indexType;
        _mapReduceBridge.documentType = delegate.getDocumentType();

        String mapVersion = delegate.getMapVersion();
        //String mapVersion = "1";
        MapReduceIndex mapReduceIndex = openIndex();

        if (mapReduceIndex == null)
            return null;
        if (mapVersion != null) {
            Transaction t = new Transaction(_indexDB);
            _index.setup(t, _indexType.getValue(), _mapReduceBridge, mapVersion);
            t.delete();
        }
        return _index;
    }

    private static String viewNames(List<ForestDBViewStore> views) {
        StringBuffer sb = new StringBuffer();
        for (ForestDBViewStore view : views) {
            sb.append(view.getName());
            sb.append(", ");
        }
        return sb.toString();
    }

    /**
     * Starts a view query, returning a CBForest enumerator.
     * - (IndexEnumerator*) _runForestQueryWithOptions: (CBLQueryOptions*)options
     */
    private IndexEnumerator runForestQuery(QueryOptions options) {
        DocEnumerator.Options forestOpts = new DocEnumerator.Options();
        forestOpts.setSkip(options.getSkip());
        if (options.getLimit() != QueryOptions.QUERY_OPTIONS_DEFAULT_LIMIT)
            forestOpts.setLimit(options.getLimit());
        forestOpts.setDescending(options.isDescending());
        forestOpts.setInclusiveStart(options.isInclusiveStart());
        forestOpts.setInclusiveEnd(options.isInclusiveEnd());

        if (options.getKeys() != null && options.getKeys().size() > 0) {
            VectorKeyRange collatableKeys = new VectorKeyRange();
            for (Object obj : options.getKeys()) {
                Collatable collKey = new Collatable();
                ForestDBCollatorUtil.add(collKey, obj);
                collatableKeys.add(new KeyRange(collKey));
            }
            return new IndexEnumerator(_index, collatableKeys, forestOpts);
        } else {
            Object endKey = Misc.keyForPrefixMatch(options.getEndKey(), options.getPrefixMatchLevel());

            Collatable collStartKey = new Collatable();
            if (options.getStartKey() != null)
                ForestDBCollatorUtil.add(collStartKey, options.getStartKey());

            Collatable collEndKey = new Collatable();
            if (endKey != null)
                ForestDBCollatorUtil.add(collEndKey, endKey);

            Slice startKeyDocID;
            if (options.getStartKeyDocId() == null)
                startKeyDocID = new Slice();
            else
                startKeyDocID = new Slice(options.getStartKeyDocId().getBytes());

            Slice endKeyDocID;
            if (options.getEndKeyDocId() == null)
                endKeyDocID = new Slice();
            else
                endKeyDocID = new Slice(options.getEndKeyDocId().getBytes());

            return new IndexEnumerator(_index,
                    collStartKey, startKeyDocID,
                    collEndKey, endKeyDocID,
                    forestOpts);
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Internal (Protected/Private) Static Methods
    ///////////////////////////////////////////////////////////////////////////

    protected static String fileNameToViewName(String fileName) {
        if (!fileName.endsWith(kViewIndexPathExtension))
            return null;
        if (fileName.startsWith("."))
            return null;
        String viewName = fileName.substring(0, fileName.indexOf("."));
        viewName = viewName.replaceAll(":", "/");
        return viewName;
    }

    private static String viewNameToFileName(String viewName) {
        if (viewName.startsWith(".") || viewName.indexOf(":") > 0)
            return null;
        viewName = viewName.replaceAll("/", ":");
        return viewName + "." + kViewIndexPathExtension;
    }

    /**
     * Are key1 and key2 grouped together at this groupLevel?
     */
    private static boolean groupTogether(Object key1, Object key2, int groupLevel) {
        if (groupLevel == 0 || !(key1 instanceof List) || !(key2 instanceof List)) {
            return key1.equals(key2);
        }
        @SuppressWarnings("unchecked")
        List<Object> key1List = (List<Object>) key1;
        @SuppressWarnings("unchecked")
        List<Object> key2List = (List<Object>) key2;

        // if either key list is smaller than groupLevel and the key lists are different
        // sizes, they cannot be equal.
        if ((key1List.size() < groupLevel || key2List.size() < groupLevel) &&
                key1List.size() != key2List.size()) {
            return false;
        }

        int end = Math.min(groupLevel, Math.min(key1List.size(), key2List.size()));
        for (int i = 0; i < end; ++i) {
            if (!key1List.get(i).equals(key2List.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the prefix of the key to use in the result row, at this groupLevel
     */
    public static Object groupKey(Object key, int groupLevel) {
        if (groupLevel > 0 && (key instanceof List) && (((List<Object>) key).size() > groupLevel)) {
            return ((List<Object>) key).subList(0, groupLevel);
        } else {
            return key;
        }
    }
}
