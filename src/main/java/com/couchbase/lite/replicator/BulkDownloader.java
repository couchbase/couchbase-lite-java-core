package com.couchbase.lite.replicator;

import com.couchbase.lite.Database;
import com.couchbase.lite.support.MultipartDocumentReader;
import com.couchbase.lite.support.MultipartReader;
import com.couchbase.lite.support.MultipartReaderDelegate;
import com.couchbase.lite.support.RemoteRequest;
import com.couchbase.lite.support.RemoteRequestCompletionBlock;
import com.couchbase.lite.util.CollectionUtils;
import com.couchbase.lite.util.Log;

import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by andy on 14/04/2014.
 */
public class BulkDownloader extends RemoteRequest implements Runnable, MultipartReaderDelegate {

    private  Database _db;
    private MultipartReader _topReader;
    private MultipartDocumentReader _docReader;
    private int _docCount;
    private BulkDownloaderDocumentBlock _onDocument;

public BulkDownloader (URL dbURL,
    Database database,
    Map<String, Object> requestHeaders,
    List revs,
    BulkDownloaderDocumentBlock onDocument,
    RemoteRequestCompletionBlock onCompletion ) throws Exception {


        super (null,
                null,
                "POST",
            new URL(dbURL,"_bulk_get?revs=true&attachments=true"),
                null, //helperMethod(revs),
            requestHeaders,
            onCompletion);

        _db = database;
        _onDocument = onDocument;

    }


    private String description() {
        return this.getClass().getName()+"["+url.getPath()+"]";
    }



    private void setupRequest: (NSMutableURLRequest*)request withBody: (id)body {
        request.HTTPBody = [CBLJSON dataWithJSONObject: body options: 0 error: NULL];
        [request addValue: @"application/json" forHTTPHeaderField: @"Content-Type"];
        [request setValue: @"multipart/related" forHTTPHeaderField: @"Accept"];
        [request setValue: @"gzip" forHTTPHeaderField: @"X-Accept-Part-Encoding"];
    }


    // URL CONNECTION CALLBACKS:


    private void connection:(NSURLConnection *)connection didReceiveResponse:(NSURLResponse *)response {
        CBLStatus status = (CBLStatus) ((NSHTTPURLResponse*)response).statusCode;
        if (status < 300) {
            // Check the content type to see whether it's a multipart response:
            NSDictionary* headers = [(NSHTTPURLResponse*)response allHeaderFields];
            NSString* contentType = headers[@"Content-Type"];
            _topReader = [[CBLMultipartReader alloc] initWithContentType: contentType
            delegate: self];
            if (!_topReader) {
                Warn(@"%@ got invalid Content-Type '%@'", self, contentType);
                [self cancelWithStatus: kCBLStatusUpstreamError];
                return;
            }
        }

        [super connection: connection didReceiveResponse: response];
    }


    private void connection:(NSURLConnection *)connection didReceiveData:(NSData *)data {
        [super connection: connection didReceiveData: data];
        [_topReader appendData: data];
        if (_topReader.error) {
            [self cancelWithStatus: kCBLStatusUpstreamError];
        }
    }


    private void connectionDidFinishLoading:(NSURLConnection *)connection {
        LogTo(SyncVerbose, @"%@: Finished loading (%u documents)", self, _docCount);
        if (!_topReader.finished) {
            Warn(@"%@ got unexpected EOF", self);
            [self cancelWithStatus: kCBLStatusUpstreamError];
            return;
        }

        clearConnection();
        respondWithResult();
    }



    //MULTIPART CALLBACKS:


/** This method is called when a part's headers have been parsed, before its data is parsed. */

     public void  startedPart(Map headers) {

        Log.v(Manager., this+": Starting new document; ID=\""+headers.get("X-Doc-ID")+"\"");
        _docReader = new MultipartDocumentReader(_db);
        _docReader.headers = headers;

    }


/** This method is called to append data to a part's body. */

    public void appendToPart(byte [] data) {

       try {
        _docReader.appendData(data);
       } catch (IllegalStateException e) {
            cancelWithStatus(_docReader.status);
        }

    }


/** This method is called when a part is complete. */

    public void finishedPart() {

        Log.v("", this + ": Finished document");
        try {
            _docReader.finish();
        } catch (IllegalStateException e) {
            cancelWithStatus(_docReader status);
        }
        ++_docCount;

    }


    public interface BulkDownloaderDocumentBlock {
        public void onDocument(Map<String, Object> props);
    }


   // private static Map<String,Object> helperMethod(Collection<T> revs, final Database database) {

        // Build up a JSON body describing what revisions we want:

        List<String> keys = [revs my_map: ^(CBL_Revision* rev) {
            BOOL hasAttachment;
            NSArray* attsSince = [_db getPossibleAncestorRevisionIDs: rev
            limit: kMaxNumberOfAttsSince
            hasAttachment: &hasAttachment];
            if (!hasAttachment || attsSince.count == 0)
                attsSince = nil;
            return $dict({@"id", rev.docID},
            {@"rev", rev.revID},
            {@"atts_since", attsSince});
        }];


   //     Map<String, Object> body = new HashMap<String, Object>();

        Collection keys = CollectionUtils.filter(revs, new CollectionUtils.Predicate() {
            @Override
            public boolean apply(Object type) {
                boolean hasAttachment = false;
                List<String> attsSince = database.getPossibleAncestorRevisionIDs(rev,
                Puller.MAX_NUMBER_OF_ATTS_SINCE,
                hasAttachment);
                if (!hasAttachment || attsSince.count == 0)
                    attsSince = nil;
                return $dict({@"id", rev.docID},
                {@"rev", rev.revID},
                {@"atts_since", attsSince});
                return false;
            }
        });

        //body.put("docs",keys);
 //       return body;
  //  }
}
