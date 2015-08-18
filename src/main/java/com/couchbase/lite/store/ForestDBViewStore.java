package com.couchbase.lite.store;

import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.QueryOptions;
import com.couchbase.lite.QueryRow;
import com.couchbase.lite.View;

import java.util.List;
import java.util.Map;

/**
 * Created by hideki on 8/13/15.
 */
public class ForestDBViewStore  implements ViewStore, QueryRowStore{
    @Override
    public boolean rowValueIsEntireDoc(byte[] valueData) {
        return false;
    }

    @Override
    public Object parseRowValue(byte[] valueData) {
        return null;
    }

    @Override
    public Map<String, Object> getDocumentProperties(String docID, long sequence) {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public ViewStoreDelegate getDelegate() {
        return null;
    }

    @Override
    public void setDelegate(ViewStoreDelegate delegate) {

    }

    @Override
    public void close() {

    }

    @Override
    public void deleteIndex() {

    }

    @Override
    public void deleteView() {

    }

    @Override
    public boolean setVersion(String version) {
        return false;
    }

    @Override
    public int getTotalRows() {
        return 0;
    }

    @Override
    public long getLastSequenceIndexed() {
        return 0;
    }

    @Override
    public long getLastSequenceChangedAt() {
        return 0;
    }

    @Override
    public void updateIndex() throws CouchbaseLiteException {

    }

    @Override
    public List<QueryRow> regularQuery(QueryOptions options) throws CouchbaseLiteException {
        return null;
    }

    @Override
    public List<QueryRow> reducedQuery(QueryOptions options) throws CouchbaseLiteException {
        return null;
    }

    @Override
    public QueryRowStore storageForQueryRow(QueryRow row) {
        return null;
    }

    @Override
    public List<Map<String, Object>> dump() {
        return null;
    }

    @Override
    public int getViewID() {
        return 0;
    }

    @Override
    public void setCollation(View.TDViewCollation collation) {

    }
}
