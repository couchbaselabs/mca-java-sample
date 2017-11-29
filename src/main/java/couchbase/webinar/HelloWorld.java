package couchbase.webinar;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.consistency.ScanConsistency;

import java.io.Console;
import java.time.Duration;
import java.util.UUID;
import java.time.Instant;
import java.util.Random;

public class HelloWorld {

        private static Bucket bucket;
        private static Integer writes;
        private static Random rand;

        public static void main(String... args) throws Exception {

                // initialize couchbase
                Cluster cluster = CouchbaseCluster.create("localhost");
                cluster.authenticate("webinaruser", "password");
                bucket = cluster.openBucket("webinar");

                // initialize counter(s) and such
                rand = new Random();
                writes = 0;

                // loop forever, doing writes
                // show the number of writes every second since the last second
                Instant start = Instant.now();
                while(true)
                {
                        Instant now = Instant.now();

                        Write();

                        if(Duration.between(start, now).getSeconds() >= 1) {
                                System.out.println("Writes: " + writes);
                                start = Instant.now();
                                writes = 0;
                        }
                }
        }

        public static void Write() {
                // Create a JSON Document
                JsonObject person = JsonObject.create()
                        .put("firstName", "John")
                        .put("lastName", "Doe")
                        .put("type", "person");

                // create a key "person::#" where # is 1 to 1000
                String key = "p::" + rand.nextInt(1000) + 1; //UUID.randomUUID().toString();

                // Store the Document
                bucket.upsert(JsonDocument.create(key, person));

                // increment the number of writes
                writes++;
        }
}
        /*
        // Initialize the Connection
        Cluster cluster = CouchbaseCluster.create("localhost");
        Bucket bucket = cluster.openBucket("default");

        //Generate a unique id for key creation
        String uniqueID = UUID.randomUUID().toString();
        String key = "p:" + uniqueID;

        // Create a JSON Document
        JsonObject person = JsonObject.create()
                .put("firstName", "John")
                .put("lastName", "Doe")
                .put("city", "San Francisco")
                .put("country", "United States")
                .put("type", "person");


        // Store the Document
        bucket.upsert(JsonDocument.create(key, person));

        // Create a N1QL Primary Index (but ignore if it exists)
        bucket.bucketManager().createN1qlPrimaryIndex(true, false);

        // Perform N1QL Query. Use request plus consistency to read your write immediately.
        N1qlQuery query =  N1qlQuery.parameterized("SELECT meta(p).id AS documentKey, p.firstName, p.lastName " +
                        "FROM default AS p WHERE type = $type",
                JsonObject.create().put("type", "person"),
                N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS));

        N1qlQueryResult result = bucket.query(query);

        for (N1qlQueryRow row : result) {
            System.out.println("Key: " + row.value().getString("documentKey") + "\t" +
                    "FirstName: " + row.value().getString("firstName") + "\t" +
                    "LastName: " + row.value().getString("lastName") + "\t"
            );
        }
        
        cluster.disconnect();
*/

