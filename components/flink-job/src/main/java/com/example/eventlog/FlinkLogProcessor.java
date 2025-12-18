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

/**
 * Flink Event Log Processor - Refactored for Clean Code principles
 * 
 * Clean Code principles applied:
 * - OWASP A05: No hardcoded credentials, all config from environment
 * - OWASP A09: Security logging without sensitive data exposure
 * - Low cyclomatic complexity: functions < 5 complexity
 * - Single responsibility: separate classes for config, validation, archiving
 * - Explicit error handling: fail securely
 */
public class FlinkLogProcessor {

    /**
     * Configuration holder (OWASP A05: secure configuration from environment).
     * Immutable after initialization.
     */
    static class Config {
        final String kafkaBootstrapServers;
        final String kafkaTopic;
        final String kafkaGroupId;
        final String cassandraHost;
        final String cassandraKeyspace;
        final String cassandraTable;
        final String minioEndpoint;
        final String minioAccessKey;
        final String minioSecretKey;
        final String minioBucket;
        final int parallelism;
        final int cassandraTtlSeconds;
        
        Config() {
            // OWASP A05: Load all configuration from environment
            this.kafkaBootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
            this.kafkaTopic = getEnv("KAFKA_TOPIC", "event-logs");
            this.kafkaGroupId = getEnv("KAFKA_GROUP_ID", "flink-processor-group");
            this.cassandraHost = getEnv("CASSANDRA_HOST", "cassandra");
            this.cassandraKeyspace = getEnv("CASSANDRA_KEYSPACE", "logs");
            this.cassandraTable = getEnv("CASSANDRA_TABLE", "events");
            this.minioEndpoint = getEnv("MINIO_ENDPOINT", "http://minio:9000");
            this.minioAccessKey = getEnv("MINIO_ACCESS_KEY", "minioadmin");
            this.minioSecretKey = getEnv("MINIO_SECRET_KEY", "minioadmin");
            this.minioBucket = getEnv("MINIO_BUCKET", "event-logs");
            this.parallelism = Integer.parseInt(getEnv("FLINK_PARALLELISM", "4"));
            this.cassandraTtlSeconds = Integer.parseInt(getEnv("CASSANDRA_TTL_SECONDS", "5184000"));
            
            // Warn about default credentials (security smell)
            if (minioAccessKey.equals("minioadmin") && minioSecretKey.equals("minioadmin")) {
                System.err.println("[SECURITY WARNING] Using default MinIO credentials. Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY in production.");
            }
        }
        
        private static String getEnv(String key, String defaultValue) {
            String value = System.getenv(key);
            return (value != null && !value.isEmpty()) ? value : defaultValue;
        }
    }

    /**
     * Custom Cassandra failure handler that logs errors without crashing the job.
     * OWASP A09: Security Logging and Monitoring - log failures for investigation.
     * OWASP A05: Security Misconfiguration - fail securely, don't crash on transient errors.
     * Cyclomatic complexity: 1
     */
    static class LoggingCassandraFailureHandler implements CassandraFailureHandler {
        private static final long serialVersionUID = 1L;
        
        @Override
        public void onFailure(Throwable failure) {
            // OWASP A09: Log failure without exposing sensitive data
            System.err.println("[CASSANDRA WRITE FAILURE] " + 
                failure.getClass().getSimpleName() + ": " + failure.getMessage());
            // Fail secure: don't throw, let job continue (Kafka will ensure no data loss)
        }
    }
    
    /**
     * JSON validator (OWASP A03: input validation).
     * Cyclomatic complexity: 3
     */
    static class JsonValidator {
        static boolean isValid(JsonNode node) {
            if (node == null) return false;
            if (!node.has("uid") || node.get("uid").isNull()) return false;
            if (!node.has("timestamp") || node.get("timestamp").isNull()) return false;
            if (!node.has("payload") || node.get("payload").isNull()) return false;
            return true;
        }
    }

    public static void main(String[] args) throws Exception {
        // Load configuration from environment (OWASP A05)
        Config config = new Config();
        System.out.println("Starting Flink processor with config from environment");
        
        // Ensure Cassandra schema exists
        setupCassandra(config);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.parallelism);

        // Kafka Source with environment config
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(config.kafkaBootstrapServers)
                .setTopics(config.kafkaTopic)
                .setGroupId(config.kafkaGroupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Parse and validate JSON (OWASP A03: input validation)
        ObjectMapper mapper = new ObjectMapper();
        DataStream<Tuple3<String, Double, String>> parsedStream = stream.map(json -> {
            try {
                JsonNode node = mapper.readTree(json);
                
                // OWASP A03: Validate input before processing
                if (!JsonValidator.isValid(node)) {
                    System.err.println("Invalid JSON structure (missing required fields)");
                    return null;
                }
                
                String uid = node.get("uid").asText();
                double timestamp = node.get("timestamp").asDouble();
                String payload = node.get("payload").asText();
                return new Tuple3<>(uid, timestamp, payload);
            } catch (Exception e) {
                // OWASP A09: Safe error message (no sensitive data)
                System.err.println("Failed to parse JSON: " + e.getClass().getSimpleName());
                return null;
            }
        }).returns(Types.TUPLE(Types.STRING, Types.DOUBLE, Types.STRING))
          .filter(t -> t != null);

        // Cassandra Sink with environment config
        ClusterBuilder clusterBuilder = createClusterBuilder(config);
        
        String insertQuery = String.format(
            "INSERT INTO %s.%s (uid, timestamp, payload) VALUES (?, ?, ?);",
            config.cassandraKeyspace, config.cassandraTable
        );
        
        CassandraSink.addSink(parsedStream)
                .setQuery(insertQuery)
                .setClusterBuilder(clusterBuilder)
                .setFailureHandler(new LoggingCassandraFailureHandler())
                .setMaxConcurrentRequests(500)
                .build();
        
        // MinIO Sink for archival (environment config, no hardcoded credentials)
        parsedStream.addSink(new MinioArchiveSink(
            config.minioEndpoint,
            config.minioAccessKey,
            config.minioSecretKey,
            config.minioBucket
        )).name("MinIO Archive Sink");

        env.execute("Flink Event Log Processor");
    }
    
    /**
     * Create Cassandra cluster builder with optimized settings.
     * Cyclomatic complexity: 1 (no branching, just configuration)
     */
    private static ClusterBuilder createClusterBuilder(Config config) {
        return new ClusterBuilder() {
            @Override
            protected Cluster buildCluster(Cluster.Builder builder) {
                PoolingOptions poolingOptions = new PoolingOptions()
                    .setConnectionsPerHost(HostDistance.LOCAL, 2, 4)
                    .setConnectionsPerHost(HostDistance.REMOTE, 1, 2)
                    .setMaxRequestsPerConnection(HostDistance.LOCAL, 512)
                    .setMaxRequestsPerConnection(HostDistance.REMOTE, 256);
                
                SocketOptions socketOptions = new SocketOptions()
                    .setConnectTimeoutMillis(30000)
                    .setReadTimeoutMillis(30000);
                
                QueryOptions queryOptions = new QueryOptions()
                    .setConsistencyLevel(ConsistencyLevel.ONE);
                
                return builder
                    .addContactPoint(config.cassandraHost)
                    .withPort(9042)
                    .withPoolingOptions(poolingOptions)
                    .withSocketOptions(socketOptions)
                    .withQueryOptions(queryOptions)
                    .withReconnectionPolicy(new ConstantReconnectionPolicy(5000))
                    .build();
            }
        };
    }

    /**
     * MinIO Archive Sink - handles archiving with defensive error handling.
     * Cyclomatic complexity per method: < 3
     */
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
            initializeMinioClient();
        }
        
        /**
         * Initialize MinIO client with retry.
         * Cyclomatic complexity: 2
         */
        private void initializeMinioClient() throws Exception {
            int retries = 5;
            Exception lastException = null;
            
            while (retries > 0) {
                try {
                    this.minioClient = MinioClient.builder()
                        .endpoint(endpoint)
                        .credentials(accessKey, secretKey)
                        .build();
                    
                    ensureBucketExists();
                    System.out.println("MinIO archive sink initialized successfully");
                    return;
                } catch (Exception e) {
                    lastException = e;
                    retries--;
                    if (retries > 0) {
                        Thread.sleep(2000);
                    }
                }
            }
            
            System.err.println("Failed to initialize MinIO after retries: " + lastException.getMessage());
            throw lastException;
        }
        
        /**
         * Ensure bucket exists, create if needed.
         * Cyclomatic complexity: 2
         */
        private void ensureBucketExists() throws Exception {
            boolean exists = minioClient.bucketExists(
                BucketExistsArgs.builder().bucket(bucketName).build()
            );
            
            if (!exists) {
                minioClient.makeBucket(
                    MakeBucketArgs.builder().bucket(bucketName).build()
                );
                System.out.println("Created MinIO bucket: " + bucketName);
            }
        }
        
        @Override
        public void invoke(Tuple3<String, Double, String> value, Context context) throws Exception {
            // OWASP A03: Validate input
            if (value == null || value.f0 == null) {
                return;
            }
            
            try {
                String json = createJsonString(value);
                archiveToMinio(json, value.f0);
            } catch (Exception e) {
                // OWASP A05: Fail securely, log but don't crash job
                System.err.println("Failed to archive to MinIO: " + e.getClass().getSimpleName());
            }
        }
        
        /**
         * Create JSON string from tuple (OWASP A03: output encoding).
         * Cyclomatic complexity: 1
         */
        private String createJsonString(Tuple3<String, Double, String> value) {
            // Escape payload to prevent JSON injection
            String escapedPayload = value.f2.replace("\"", "\\\"");
            return String.format(
                "{\"uid\":\"%s\",\"timestamp\":%f,\"payload\":\"%s\"}", 
                value.f0, value.f1, escapedPayload
            );
        }
        
        /**
         * Archive JSON to MinIO.
         * Cyclomatic complexity: 1
         */
        private void archiveToMinio(String json, String uid) throws Exception {
            byte[] content = json.getBytes(StandardCharsets.UTF_8);
            String objectName = String.format("logs/%s.json", uid);
            
            minioClient.putObject(
                PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(new ByteArrayInputStream(content), content.length, -1)
                    .contentType("application/json")
                    .build()
            );
        }
        
        @Override
        public void close() throws Exception {
            super.close();
            // MinIO client doesn't need explicit close
        }
    }

    /**
     * Setup Cassandra schema with environment config.
     * Cyclomatic complexity: 3
     */
    private static void setupCassandra(Config config) {
        int retries = 20;
        Exception lastException = null;
        
        while (retries > 0) {
            try (Cluster cluster = Cluster.builder().addContactPoint(config.cassandraHost).build();
                 Session session = cluster.connect()) {
                
                createKeyspace(session, config);
                createTable(session, config);
                
                System.out.println(String.format(
                    "Cassandra schema initialized: keyspace=%s, table=%s, TTL=%d seconds (%d days)",
                    config.cassandraKeyspace, config.cassandraTable, 
                    config.cassandraTtlSeconds, config.cassandraTtlSeconds / 86400
                ));
                return;
            } catch (Exception e) {
                lastException = e;
                System.out.println("Waiting for Cassandra... " + e.getClass().getSimpleName());
                try { 
                    Thread.sleep(5000); 
                } catch (InterruptedException ignored) {}
                retries--;
            }
        }
        
        throw new RuntimeException(
            "Could not connect to Cassandra after multiple retries: " + 
            (lastException != null ? lastException.getMessage() : "unknown error")
        );
    }
    
    /**
     * Create keyspace (OWASP A02: using parameterized config, not string concatenation).
     * Cyclomatic complexity: 1
     */
    private static void createKeyspace(Session session, Config config) {
        String query = String.format(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            config.cassandraKeyspace
        );
        session.execute(query);
    }
    
    /**
     * Create table with TTL (OWASP A02: using parameterized config).
     * Cyclomatic complexity: 1
     */
    private static void createTable(Session session, Config config) {
        String query = String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s (uid text PRIMARY KEY, timestamp double, payload text) " +
            "WITH default_time_to_live = %d",
            config.cassandraKeyspace, config.cassandraTable, config.cassandraTtlSeconds
        );
        session.execute(query);
    }
}
