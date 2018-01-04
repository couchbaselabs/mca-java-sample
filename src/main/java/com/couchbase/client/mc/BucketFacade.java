package com.couchbase.client.mc;

import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;

import java.util.concurrent.TimeUnit;

public class BucketFacade {
  private final MultiClusterBucket bucket;
  private final long timeout;
  private final TimeUnit timeUnit;

  @SuppressWarnings("unused")
  private BucketFacade() { throw new AssertionError("This is not the constructor you are looking for."); }

  public BucketFacade(MultiClusterBucket bucket, long timeout, TimeUnit timeUnit) {
    this.bucket = bucket;
    this.timeout = timeout;
    this.timeUnit = timeUnit;
  }
  
  public boolean exists(String id) { return bucket.exists(id, timeout, timeUnit); }

  public JsonDocument get(String id) { return bucket.get(id, timeout, timeUnit); }

  public <D extends Document<?>> D insert(D document) { return bucket.insert(document, timeout, timeUnit); }

  public <D extends Document<?>> D upsert(D document) { return bucket.upsert(document, timeout, timeUnit); }
  
  public N1qlQueryResult query(N1qlQuery query) { return bucket.query(query, timeout, timeUnit); }
}