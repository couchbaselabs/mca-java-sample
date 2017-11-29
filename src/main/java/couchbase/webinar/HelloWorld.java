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
import com.github.javafaker.Faker;

public class HelloWorld {

        private static Bucket bucket;
        private static Integer writes;
        private static Integer reads;
        private static Random rand;
        private static Faker faker;

        public static void main(String... args) throws Exception {

                // initialize couchbase
                Cluster cluster = CouchbaseCluster.create("localhost");
                cluster.authenticate("webinaruser", "password");
                bucket = cluster.openBucket("webinar");

                // initialize counter(s) and such
                rand = new Random();
                faker = new Faker();
                writes = 0;
                reads = 0;

                // loop forever, doing reads and writes
                Instant start = Instant.now();
                while(true)
                {
                        Instant now = Instant.now();

                        // write 33% of the time, read 67% of the time
                        Integer split = rand.nextInt(3);
                        if(split == 1)
                                Write();
                        else
                                Read();

                        // if 1 second has elapsed, show the counts and reset them for the
                        // next second
                        if(Duration.between(start, now).getSeconds() >= 1) {
                                System.out.println("Writes: " + writes + ", Reads: " + reads);
                                start = Instant.now();
                                writes = 0;
                                reads = 0;
                        }
                }
        }

        public static void Write() {
                // Create a JSON Document
                JsonObject person = JsonObject.create()
                        .put("firstName", faker.name().firstName())
                        .put("lastName", faker.name().lastName())
                        .put("type", "person");

                // create a key "person::#" where # is 1 to 1000
                String key = "p::" + rand.nextInt(1000) + 1;

                // Store the Document
                bucket.upsert(JsonDocument.create(key, person));

                // increment the number of writes
                writes++;
        }

        public static void Read() {
                // create a key "person::#" where # is 1 to 1000
                String key = "p::" + rand.nextInt(1000) + 1;

                // Store the Document
                bucket.get(key);

                // increment the number of writes
                reads++;
        }
}
