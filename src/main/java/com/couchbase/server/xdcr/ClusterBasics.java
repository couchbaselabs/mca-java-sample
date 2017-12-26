package com.couchbase.server.xdcr;

import com.couchbase.utils.Options;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.Statement;

import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.*;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import com.github.javafaker.Faker;
import com.github.javafaker.Name;

/**
 * Multi-threaded read/write and dynamic query load generator for Couchbase, supporting multiple data centers.  The data
 * centers will typically be set to replicate using Couchbase's Cross-Data Center Replication (xDCR) feature.
 */

public class ClusterBasics {
  private static Bucket bucket;
  private static String bucketName;

  private static final int MAX_ID = 1000000; 

  private static AtomicLong reads = new AtomicLong(0);
  private static AtomicLong writes = new AtomicLong(0);
  private static AtomicLong updates = new AtomicLong(0);
  private static AtomicLong queries = new AtomicLong(0);

  private static boolean verbose = false;
  private static final Faker faker = new Faker();

  public static void main(String... args) throws Exception {
    Options options = new Options(args);

    verbose = options.has("verbose");
    bucketName = options.stringValueOf("bucket");

    Cluster cluster = CouchbaseCluster.create(options.stringValueOf("cluster").split(","));
    cluster.authenticate(options.stringValueOf("user"), options.stringValueOf("password"));
    bucket = cluster.openBucket(bucketName);

    startThreads(options);

    statistics();
  }

  public static class Reader implements Runnable {
    @Override
    public void run() {
      while (true) {
        int id = ThreadLocalRandom.current().nextInt(MAX_ID);

        try {
          bucket.get("person::" + id);
          reads.getAndIncrement();
        } catch(Exception ex) {
          if (verbose) System.err.println("Reader exception: " + ex.getMessage());
        }
      }
    }
  }

  public static class Writer implements Runnable {
    @Override
    public void run() {
      while (true) {
        Name name = faker.name();

        JsonObject contents = JsonObject.create()
            .put("firstName", name.firstName())
            .put("lastName", name.lastName())
            .put("type", "person");

        String key = "person::" + UUID.randomUUID().toString();

        if (verbose) System.out.println("Wrote key " + key);

        bucket.insert(JsonDocument.create(key, contents));

        writes.getAndIncrement();
      }
    }
  }

  public static class Updater implements Runnable {
    @Override
    public void run() {
      while (true) {
        int id = ThreadLocalRandom.current().nextInt(MAX_ID);
      
        try {
          JsonDocument document = bucket.get("person::" + id);
          Name name = faker.name();          
          
          document.content()
            .put("firstName", name.firstName())
            .put("lastName", name.lastName());
          
          bucket.upsert(document);

          updates.getAndIncrement();
        } catch(Exception ex) {
          if (verbose) System.err.println("Updater exception: " + ex.getMessage());
        }
      }
    }
  }

  public static class Query implements Runnable {
    @Override
    public void run() {
      while (true) {
        Name name = faker.name();

        Statement statement = select("firstName")
          .from(bucketName)
          .where(x("lastName").eq(s(name.lastName())));
        N1qlQuery query = N1qlQuery.simple(statement);
        N1qlQueryResult result = bucket.query(query);

        queries.getAndIncrement();

        if (!verbose) continue;
        
        System.out.println("Queried for " + name.lastName());

        for (N1qlQueryRow row : result) {
          System.out.println(row);
        }
      }
    }
  }

  private static void startThreads(Options options) {
    for (int nn = options.integerValueOf("readers"); nn > 0; --nn) {
      new Thread(new ClusterBasics.Reader()).start();
    }

    for (int nn = options.integerValueOf("writers"); nn > 0; --nn) {
      new Thread(new ClusterBasics.Writer()).start();
    }

    for (int nn = options.integerValueOf("updaters"); nn > 0; --nn) {
      new Thread(new ClusterBasics.Updater()).start();
    }

    for (int nn = options.integerValueOf("queries"); nn > 0; --nn) {
      new Thread(new ClusterBasics.Query()).start();
    }
  }

  private static void statistics() throws InterruptedException {
    long read, aggregateReads = 0;
    long written, aggregateWrites = 0;
    long updated, aggregateUpdates = 0;
    long queried, aggregateQueries = 0;

    long elapsedTime, startTime = System.currentTimeMillis();

    while (true) {
      Thread.sleep(1000);
      
      read = reads.getAndSet(0);
      written = writes.getAndSet(0);
      updated = updates.getAndSet(0);
      queried = queries.getAndSet(0);

      elapsedTime = System.currentTimeMillis() - startTime;

      aggregateReads += read;
      aggregateWrites += written;
      aggregateUpdates += updated;
      aggregateQueries += queried;

      if (((elapsedTime/1000) % 10) == 1) {
        System.out.format("-- Average ops/second --%n%d reads %d writes %d updates %d queries%n",
        1000*aggregateReads/elapsedTime,
        1000*aggregateWrites/elapsedTime,
        1000*aggregateUpdates/elapsedTime,
        1000*aggregateQueries/elapsedTime);
      }

      if (((elapsedTime/1000) % 5) == 1) {
        System.out.println("-- Ops/second --");
      }

      System.out.format("%d reads %d writes %d updates %d queries%n",
          read, written, updated, queried);
    }
  }
}
