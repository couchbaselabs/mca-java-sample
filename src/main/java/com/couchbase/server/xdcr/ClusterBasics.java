package com.couchbase.server.xdcr;

import com.couchbase.utils.Options;
import com.couchbase.utils.Logger;
import static com.couchbase.utils.Logger.log;

import com.couchbase.client.mc.ClusterSpec;
import com.couchbase.client.mc.MultiClusterClient;
import com.couchbase.client.mc.BucketFacade;
import com.couchbase.client.mc.coordination.Coordinator;
import com.couchbase.client.mc.coordination.Coordinators;
import com.couchbase.client.mc.coordination.IsolatedCoordinator;
import com.couchbase.client.mc.coordination.TopologyBehavior;
import com.couchbase.client.mc.detection.FailureDetectorFactory;
import com.couchbase.client.mc.detection.NodeHealthFailureDetector;
import com.couchbase.client.mc.detection.DisjunctionFailureDetectorFactory;
import com.couchbase.client.mc.detection.FailureDetectorFactories;
import com.couchbase.client.mc.detection.TrafficMonitoringFailureDetector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.core.service.ServiceType;
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
import java.util.stream.Collectors;

import com.github.javafaker.Faker;
import com.github.javafaker.Name;

/**
 * Multi-threaded read/write and dynamic query load generator for Couchbase, supporting multiple data centers.  The data
 * centers will typically be set to replicate using Couchbase's Cross-Data Center Replication (xDCR) feature.
 */

public class ClusterBasics {
  private static final int TIMEOUT = 1000;

  private static String bucketName;
  private static BucketFacade bucket;

  private static final Set<ServiceType> serviceTypes = new HashSet<>();

  private static final int MAX_ID = 1000000; 

  private static AtomicLong reads = new AtomicLong(0);
  private static AtomicLong writes = new AtomicLong(0);
  private static AtomicLong updates = new AtomicLong(0);
  private static AtomicLong queries = new AtomicLong(0);

  private static boolean verbose = false;
  private static final Faker faker = new Faker();

  public static void main(String... args) throws Exception {
    Options options = new Options(args);

    Logger.init(options.stringValueOf("log"));
    verbose = options.has("verbose");
    
    bucketName = options.stringValueOf("bucket");

    // tag::cluster-spec[]
    String[] clusters = options.stringValueOf("clusters").split(":");

    List<ClusterSpec> specs = new ArrayList<>(clusters.length);

    for (String cluster: clusters) {
      Set<String> nodes = Arrays.stream(cluster.split(",")).collect(Collectors.toSet());
      specs.add(ClusterSpec.create(nodes));
    }
    // end::cluster-spec[]

    // tag::services[]
    serviceTypes.add(ServiceType.BINARY);
    serviceTypes.add(ServiceType.QUERY);
    // end::services[]

    // tag::coordinator[]
    Coordinator coordinator = Coordinators.isolated(new IsolatedCoordinator.Options() // <1>
      .clusterSpecs(specs) // <2>
      .activeEntries(specs.size()) // <3>
      .failoverNumNodes(2) // <4>
      .gracePeriod(TIMEOUT) // <5>
      .topologyBehavior(TopologyBehavior.WRAP_AT_END) // <6>
      .serviceTypes(serviceTypes) // <7>
    );
    // end::coordinator[]

    // tag::failure-detector[]
    TrafficMonitoringFailureDetector.Options trafficOptions = TrafficMonitoringFailureDetector.options() // <1>
      .maxFailedOperations(5) // <2>
      .failureInterval(60); // <3>

    FailureDetectorFactory<TrafficMonitoringFailureDetector> traffic = FailureDetectorFactories.trafficMonitoring(coordinator, trafficOptions); // <4>

    NodeHealthFailureDetector.Options healthOptions = NodeHealthFailureDetector.options();
    
    FailureDetectorFactory<NodeHealthFailureDetector> health = FailureDetectorFactories.nodeHealth(coordinator, healthOptions); // <5>

    DisjunctionFailureDetectorFactory detector = FailureDetectorFactories.disjunction(traffic, health); // <6>
    // end::failure-detector[]

    // tag::client[]
    MultiClusterClient client = new MultiClusterClient(coordinator, detector);
  
    client.authenticate(options.stringValueOf("id"), options.stringValueOf("password"));
    bucket = new BucketFacade(client.openBucket(bucketName, null), TIMEOUT, TimeUnit.MILLISECONDS);
    // end::client[]

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
          log("read", "Reader exception: ", ex.getMessage());
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

        try {
          bucket.insert(JsonDocument.create(key, contents));
          writes.getAndIncrement();

          if (verbose) System.out.println("Wrote key " + key);
        } catch(Exception ex) {
          log("write", "Writer exception: ", ex.getMessage());
        }
      }
    }
  }

  public static class Updater implements Runnable {
    @Override
    public void run() {
      while (true) {
        int id = ThreadLocalRandom.current().nextInt(MAX_ID);
      
        try {
          Name name = faker.name();
          JsonDocument document = bucket.get("person::" + id);

          if (null == document) {
            document = JsonDocument.create("person::" + id, JsonObject.create().put("type", "person"));
          }
          
          document.content()
            .put("firstName", name.firstName())
            .put("lastName", name.lastName());
          
          bucket.upsert(document);

          updates.getAndIncrement();
        } catch(Exception ex) {
          log("update", "Updater exception: ", ex.getMessage());
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
        
        try {
          N1qlQueryResult result = bucket.query(query);
          queries.getAndIncrement();

          if (!verbose) continue;
        
          System.out.println("Queried for " + name.lastName());

          for (N1qlQueryRow row : result) {
            System.out.println(row);
          }
        } catch(Exception ex) {
          log("query", "Query exception: ", ex.getMessage());
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

      if (!Logger.logging("statistics")) continue;

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
