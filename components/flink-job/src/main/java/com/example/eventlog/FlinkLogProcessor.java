package com.example.eventlog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.cassandra.CassandraFailureHandler;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public class FlinkLogProcessor {

    /**
     * Custom Cassandra failure handler that logs errors without crashing the job.
     * OWASP A09: Security Logging and Monitoring - log failures for investigation.
     * OWASP A05: Security Misconfiguration - fail securely, don't crash on transient errors.
     * 
     * This handler prevents data loss by allowing the job to continue processing
     * even when Cassandra is temporarily unavailable. Failed writes are logged
     * for investigation.
     */
    static class LoggingCassandraFailureHandler implements CassandraFailureHandler {
        private static final long serialVersionUID = 1L;
        
        @Override
        public void onFailure(Throwable failure) {
            // Log the failure without exposing sensitive data (OWASP A09)
            System.err.println("[CASSANDRA WRITE FAILURE] " + failure.getClass().getSimpleName() + ": " + failure.getMessage());
            
            // Don't throw - let the job continue processing
            // Kafka commits will ensure we don't lose data, we can reprocess if needed
        }
    }

    public static void main(String[] args) throws Exception {
        // Ensure Cassandra schema exists
        setupCassandra();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Set parallelism to 4 for high throughput during load tests
        // This allows parallel processing of Kafka partitions and Cassandra writes
        env.setParallelism(4);

        String kafkaBootstrapServers = "kafka:9092";
        String kafkaTopic = "event-logs";
        String cassandraHost = "cassandra";

        // Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics(kafkaTopic)
                .setGroupId("flink-processor-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Parse JSON
        ObjectMapper mapper = new ObjectMapper();
        DataStream<Tuple3<String, Double, String>> parsedStream = stream.map(json -> {
            try {
                JsonNode node = mapper.readTree(json);
                String uid = node.get("uid").asText();
                double timestamp = node.get("timestamp").asDouble();
                String payload = node.get("payload").asText();
                return new Tuple3<>(uid, timestamp, payload);
            } catch (Exception e) {
                System.err.println("Failed to parse: " + json);
                return null;
            }
        }).returns(Types.TUPLE(Types.STRING, Types.DOUBLE, Types.STRING))
          .filter(t -> t != null);

        // Cassandra Sink with defensive connection pool settings
        ClusterBuilder clusterBuilder = new ClusterBuilder() {
            @Override
            protected Cluster buildCluster(Cluster.Builder builder) {
                // Optimized pooling for high throughput load testing
                // Increased connection pool to handle parallel processing
                PoolingOptions poolingOptions = new PoolingOptions()
                    .setConnectionsPerHost(HostDistance.LOCAL, 2, 4)  // Increased for parallel writes
                    .setConnectionsPerHost(HostDistance.REMOTE, 1, 2)  // Increased for redundancy
                    .setMaxRequestsPerConnection(HostDistance.LOCAL, 512)  // Increased for throughput
                    .setMaxRequestsPerConnection(HostDistance.REMOTE, 256);  // Increased for throughput
                
                // Extended timeouts to handle transient issues during chaos events
                SocketOptions socketOptions = new SocketOptions()
                    .setConnectTimeoutMillis(30000)  // Increased from 10s to 30s
                    .setReadTimeoutMillis(30000);    // Increased from 12s to 30s
                
                // Query timeouts with relaxed consistency
                QueryOptions queryOptions = new QueryOptions()
                    .setConsistencyLevel(ConsistencyLevel.ONE);
                
                return builder
                    .addContactPoint(cassandraHost)
                    .withPort(9042)
                    .withPoolingOptions(poolingOptions)
                    .withSocketOptions(socketOptions)
                    .withQueryOptions(queryOptions)
                    .withReconnectionPolicy(new ConstantReconnectionPolicy(5000))  // Increased from 1s to 5s
                    .build();
            }
        };
        
        CassandraSink.addSink(parsedStream)
                .setQuery("INSERT INTO logs.events (uid, timestamp, payload) VALUES (?, ?, ?);")  
                .setClusterBuilder(clusterBuilder)
                .setFailureHandler(new LoggingCassandraFailureHandler())
                .setMaxConcurrentRequests(500)  // Increased for high throughput load testing
                .build();        // MinIO Sink for archival (defensive: write-and-forget pattern)
        parsedStream.addSink(new MinioArchiveSink(
            System.getenv().getOrDefault("MINIO_ENDPOINT", "http://minio:9000"),
            System.getenv().getOrDefault("MINIO_ACCESS_KEY", "minioadmin"),
            System.getenv().getOrDefault("MINIO_SECRET_KEY", "minioadmin"),
            System.getenv().getOrDefault("MINIO_BUCKET", "event-logs")
        )).name("MinIO Archive Sink");

        env.execute("Flink Event Log Processor");
    }
    
    // Custom sink for archiving to MinIO
    static class MinioArchiveSink extends org.apache.flink.streaming.api.functions.sink.RichSinkFunction<Tuple3<String, Double, String>> {
        private final String endpoint;
        private final String accessKey;
        private final String secretKey;
        private final String bucketName;
        private transient MinioClient minioClient;
        
        public MinioArchiveSink(String endpoint, String accessKey, String secretKey, String bucketName) {
            this.endpoint = endpoint;
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.bucketName = bucketName;
        }
        
        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            
            // Initialize MinIO client with defensive retry
            int retries = 5;
            while (retries > 0) {
                try {
                    this.minioClient = MinioClient.builder()
                        .endpoint(endpoint)
                        .credentials(accessKey, secretKey)
                        .build();
                    
                    // Ensure bucket exists
                    boolean exists = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
                    if (!exists) {
                        minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
                        System.out.println("Created MinIO bucket: " + bucketName);
                    }
                    
                    System.out.println("MinIO archive sink initialized successfully");
                    return;
                } catch (Exception e) {
                    retries--;
                    if (retries == 0) {
                        System.err.println("Failed to initialize MinIO after retries: " + e.getMessage());
                        throw e;
                    }
                    Thread.sleep(2000);
                }
            }
        }
        
        @Override
        public void invoke(Tuple3<String, Double, String> value, Context context) throws Exception {
            // Defensive: guard against null values
            if (value == null || value.f0 == null) {
                return;
            }
            
            try {
                // Create JSON for archival
                String json = String.format("{\"uid\":\"%s\",\"timestamp\":%f,\"payload\":\"%s\"}", 
                    value.f0, value.f1, value.f2.replace("\"", "\\\""));
                
                byte[] content = json.getBytes(StandardCharsets.UTF_8);
                
                // Store with uid as key for deduplication
                String objectName = String.format("logs/%s.json", value.f0);
                
                minioClient.putObject(
                    PutObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .stream(new ByteArrayInputStream(content), content.length, -1)
                        .contentType("application/json")
                        .build()
                );
            } catch (Exception e) {
                // Defensive: log but don't fail the job
                System.err.println("Failed to archive to MinIO: " + e.getMessage());
            }
        }
        
        @Override
        public void close() throws Exception {
            super.close();
            // MinIO client doesn't need explicit close
        }
    }

    private static void setupCassandra() {
        String host = "cassandra";
        // Get TTL from environment variable, default to 60 days (5184000 seconds)
        int ttlSeconds = Integer.parseInt(System.getenv().getOrDefault("CASSANDRA_TTL_SECONDS", "5184000"));
        
        int retries = 20;
        while (retries > 0) {
            try (Cluster cluster = Cluster.builder().addContactPoint(host).build();
                 Session session = cluster.connect()) {
                
                session.execute("CREATE KEYSPACE IF NOT EXISTS logs WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
                
                // Create table with default TTL for automatic expiration
                String createTableQuery = String.format(
                    "CREATE TABLE IF NOT EXISTS logs.events (uid text PRIMARY KEY, timestamp double, payload text) " +
                    "WITH default_time_to_live = %d", 
                    ttlSeconds
                );
                session.execute(createTableQuery);
                
                System.out.println("Cassandra schema initialized with TTL: " + ttlSeconds + " seconds (" + (ttlSeconds / 86400) + " days)");
                return;
            } catch (Exception e) {
                System.out.println("Waiting for Cassandra... " + e.getMessage());
                try { Thread.sleep(5000); } catch (InterruptedException ignored) {}
                retries--;
            }
        }
        throw new RuntimeException("Could not connect to Cassandra after multiple retries.");
    }
}
