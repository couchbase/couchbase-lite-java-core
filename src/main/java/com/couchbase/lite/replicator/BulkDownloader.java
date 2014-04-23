package com.couchbase.lite.replicator;

import com.couchbase.lite.Database;
import com.couchbase.lite.internal.InterfaceAudience;
import com.couchbase.lite.internal.RevisionInternal;
import com.couchbase.lite.support.MultipartDocumentReader;
import com.couchbase.lite.support.MultipartReaderDelegate;
import com.couchbase.lite.support.RemoteRequest;
import com.couchbase.lite.support.RemoteRequestCompletionBlock;
import com.couchbase.lite.util.CollectionUtils;
import com.couchbase.lite.util.Log;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;

import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class BulkDownloader extends RemoteRequest implements Runnable, MultipartReaderDelegate {

    private Database _db;
    private MultipartDocumentReader _docReader;
    private int _docCount;
    private BulkDownloaderDocumentBlock _onDocument;
    private HttpResponse response;

    public BulkDownloader(URL dbURL,
                          Database database,
                          Map<String, Object> requestHeaders,
                          List<RevisionInternal> revs,
                          BulkDownloaderDocumentBlock onDocument,
                          RemoteRequestCompletionBlock onCompletion) throws Exception {

        super(null,
                null,
                "POST",
                new URL(buildRelativeURLString(dbURL, "/_bulk_get?revs=true&attachments=true")),
                helperMethod(revs,database),
                requestHeaders,
                onCompletion);

        response = null;

        setOnPreCompletion(new RemoteRequestCompletionBlock() {
            @Override
            public void onCompletion(Object result, Throwable e) {
                response = (HttpResponse) result;
            }
        });

        _db = database;
        _onDocument = onDocument;

    }


    private String description() {
        return this.getClass().getName() + "[" + url.getPath() + "]";
    }

    @Override
    protected void addRequestHeaders(HttpUriRequest request) {

        requestHeaders.put("Content-Type", "application/json");
        requestHeaders.put("Accept", "multipart/related");
        requestHeaders.put("X-Accept-Part-Encoding", "gzip");

        for (String requestHeaderKey : requestHeaders.keySet()) {
            request.addHeader(requestHeaderKey, requestHeaders.get(requestHeaderKey).toString());
        }
    }

    /**
     * This method is called when a part's headers have been parsed, before its data is parsed.
     */

    public void startedPart(Map headers) {
        if (_docReader != null) {
            throw new IllegalStateException("_docReader is already defined");
        }
        Log.v(Log.TAG_SYNC, "%s: Starting new document; ID=%s", headers.get("X-Doc-ID"), this);
        _docReader = new MultipartDocumentReader(response, _db);
        _docReader.startedPart(headers);

    }


    /**
     * This method is called to append data to a part's body.
     */

    public void appendToPart(byte[] data) {
        if (_docReader == null) {
            throw new IllegalStateException("_docReader is not defined");
        }
        _docReader.appendData(data);
    }


    /**
     * This method is called when a part is complete.
     */

    public void finishedPart() {
        Log.v(Log.TAG_SYNC, "%s: Finished document", this);
        if (_docReader == null) {
            throw new IllegalStateException("_docReader is not defined");
        }

        _docReader.finish();
        ++_docCount;
        _onDocument.onDocument(_docReader.getDocumentProperties());
        _docReader = null;
    }


    public interface BulkDownloaderDocumentBlock {
        public void onDocument(Map<String, Object> props);
    }


    private static Map<String, Object> helperMethod(List<RevisionInternal> revs, final Database database) {

        // Build up a JSON body describing what revisions we want:
        Collection<Map<String, Object>> keys = CollectionUtils.transform(revs, new CollectionUtils.Functor<RevisionInternal, Map<String, Object>>() {

            public Map<String, Object> invoke(RevisionInternal source) {
                AtomicBoolean hasAttachment = new AtomicBoolean(false);
                List<String> attsSince = database.getPossibleAncestorRevisionIDs(source, Puller.MAX_NUMBER_OF_ATTS_SINCE, hasAttachment);
                if (!hasAttachment.get() || attsSince.size() == 0) {
                    attsSince = null;
                }
                Map<String, Object> mapped = new HashMap<String, Object>();
                mapped.put("id", source.getDocId());
                mapped.put("rev", source.getRevId());
                mapped.put("atts_since", attsSince);

                return mapped;
            }
        });

        Map<String, Object> retval = new HashMap<String, Object>();
        retval.put("docs", keys);
        return retval;
    }

    @InterfaceAudience.Private
    private static String buildRelativeURLString(URL remote, String relativePath) {

        // the following code is a band-aid for a system problem in the codebase
        // where it is appending "relative paths" that start with a slash, eg:
        //     http://dotcom/db/ + /relpart == http://dotcom/db/relpart
        // which is not compatible with the way the java url concatonation works.
        String remoteUrlString = remote.toExternalForm();
        if (remoteUrlString.endsWith("/") && relativePath.startsWith("/")) {
            remoteUrlString = remoteUrlString.substring(0, remoteUrlString.length() - 1);
        }
        return remoteUrlString + relativePath;
    }
}
