/**
 * Original iOS version by  Jens Alfke
 * Ported to Android by Marty Schoch
 * <p/>
 * Copyright (c) 2012 Couchbase, Inc. All rights reserved.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.couchbase.lite;

import java.util.List;

/**
 * Standard query options for views.
 *
 * @exclude
 */
public class QueryOptions {

    public static int QUERY_OPTIONS_DEFAULT_LIMIT = Integer.MAX_VALUE;

    private Object startKey = null;
    private Object endKey = null;
    private List<Object> keys = null;
    private int skip = 0;
    private int limit = QUERY_OPTIONS_DEFAULT_LIMIT;
    private int groupLevel = 0;
    private int prefixMatchLevel = 0;
    private boolean descending = false;
    private boolean includeDocs = false;

    private boolean updateSeq = false;
    private boolean inclusiveEnd = true;
    private boolean reduce = false;
    private boolean reduceSpecified = false;
    private boolean group = false;
    private Query.IndexUpdateMode stale = Query.IndexUpdateMode.BEFORE;
    private Query.AllDocsMode allDocsMode;

    private String startKeyDocId;
    private String endKeyDocId;

    private Predicate<QueryRow> postFilter;


    public Object getStartKey() {
        return startKey;
    }

    public void setStartKey(Object startKey) {
        this.startKey = startKey;
    }

    public Object getEndKey() {
        return endKey;
    }

    public void setEndKey(Object endKey) {
        this.endKey = endKey;
    }

    public int getSkip() {
        return skip;
    }

    public void setSkip(int skip) {
        this.skip = skip;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public boolean isDescending() {
        return descending;
    }

    public void setDescending(boolean descending) {
        this.descending = descending;
    }

    public boolean isIncludeDocs() {
        return includeDocs;
    }

    public void setIncludeDocs(boolean includeDocs) {
        this.includeDocs = includeDocs;
    }

    public boolean isUpdateSeq() {
        return updateSeq;
    }

    public void setUpdateSeq(boolean updateSeq) {
        this.updateSeq = updateSeq;
    }

    public boolean isInclusiveEnd() {
        return inclusiveEnd;
    }

    public void setInclusiveEnd(boolean inclusiveEnd) {
        this.inclusiveEnd = inclusiveEnd;
    }

    public int getGroupLevel() {
        return groupLevel;
    }

    public void setGroupLevel(int groupLevel) {
        this.groupLevel = groupLevel;
    }

    public int getPrefixMatchLevel() {
        return prefixMatchLevel;
    }

    public void setPrefixMatchLevel(int prefixMatchLevel) {
        this.prefixMatchLevel = prefixMatchLevel;
    }

    public boolean isReduce() {
        return reduce;
    }

    public void setReduce(boolean reduce) {
        this.reduce = reduce;
    }

    public boolean isGroup() {
        return group;
    }

    public void setGroup(boolean group) {
        this.group = group;
    }

    public List<Object> getKeys() {
        return keys;
    }

    public void setKeys(List<Object> keys) {
        this.keys = keys;
    }

    public Query.IndexUpdateMode getStale() {
        return stale;
    }

    public void setStale(Query.IndexUpdateMode stale) {
        this.stale = stale;
    }

    public boolean isReduceSpecified() {
        return reduceSpecified;
    }

    public void setReduceSpecified(boolean reduceSpecified) {
        this.reduceSpecified = reduceSpecified;
    }

    public Query.AllDocsMode getAllDocsMode() {
        return allDocsMode;
    }

    public void setAllDocsMode(Query.AllDocsMode allDocsMode) {
        this.allDocsMode = allDocsMode;
    }

    public String getStartKeyDocId() {
        return startKeyDocId;
    }

    public void setStartKeyDocId(String startKeyDocId) {
        this.startKeyDocId = startKeyDocId;
    }

    public String getEndKeyDocId() {
        return endKeyDocId;
    }

    public void setEndKeyDocId(String endKeyDocId) {
        this.endKeyDocId = endKeyDocId;
    }

    public Predicate<QueryRow> getPostFilter() {
        return postFilter;
    }

    public void setPostFilter(Predicate<QueryRow> postFilter) {
        this.postFilter = postFilter;
    }

}
