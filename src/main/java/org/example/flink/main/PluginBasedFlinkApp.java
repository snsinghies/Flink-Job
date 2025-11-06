package org.example.flink;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

/**
 * Plugin-Based Flink Application
 * Switch between MongoDB/Kafka sources and Kafka/Doris sinks using configuration
 */
public class PluginBasedFlinkApp {
   /* public static void main(String[] args) throws Exception {
       // PluginExecutor.getInstance(args).execute();


       
    }*/

       public static void main(String[] args) throws Exception {
        // Step 1: Ensure DB + Table exists
        initDatabase();

        Thread.sleep(30);
        // Step 2: Setup MySQL CDC Source
   MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
        .hostname("mysql")               // <--- use service name, not localhost
        .port(3306)
        .databaseList("mydb")
        .tableList("mydb.users")
        .username("flinkuser")
        .password("flinkpw")
        .deserializer(new JsonDebeziumDeserializationSchema())
        .startupOptions(StartupOptions.initial())
        .build();

            System.out.println("MySqlSource  has set !===>");



        // Step 3: Flink Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        // Step 4: Read MySQL changes
        DataStreamSource<String> sourceStream = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL Source"
        );

        System.out.println("DataStreamSource  has set !===>");


        // Step 5: Write to Kafka
        Properties kafkaProps = new Properties();
        //kafkaProps.setProperty("bootstrap.servers", "kafka:9092");
        kafkaProps.setProperty("bootstrap.servers", "kafka:29092");
        kafkaProps.setProperty("auto.create.topics.enable","true");

        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>(
                "user-changes",               // target topic
                new org.apache.flink.api.common.serialization.SimpleStringSchema(),
                kafkaProps
        );
       System.out.println("FlinkKafkaProducer  has set !===>");


        sourceStream.addSink(kafkaSink);

        env.execute("MySQL CDC to Kafka POC");

        System.out.println("MySQL CDC to Kafka POC !===>");

    }

    private static void initDatabase() {
    System.out.println("initDatabase  has invocked !===>");

        String url = "jdbc:mysql://mysql:3306/?user=root&password=root"; // root credentials for setup
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {

            // Create database if not exists
            System.out.println("Connection has created !===>");
            stmt.execute("CREATE DATABASE IF NOT EXISTS mydb");

            // Create flink user if not exists (with required privileges)
            stmt.execute("CREATE USER IF NOT EXISTS 'flinkuser'@'%' IDENTIFIED BY 'flinkpw'");
            stmt.execute("GRANT ALL PRIVILEGES ON mydb.* TO 'flinkuser'@'%'");
            stmt.execute("GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'flinkuser'@'%'");
            stmt.execute("FLUSH PRIVILEGES");

            // Create table if not exists
            stmt.execute("CREATE TABLE IF NOT EXISTS mydb.users (" +
                    "id INT AUTO_INCREMENT PRIMARY KEY," +
                    "name VARCHAR(255)," +
                    "email VARCHAR(255)" +
                    ")");

            // Insert some sample data if empty
            stmt.execute("INSERT INTO mydb.users (name, email) " +
                    "SELECT 'Alice', 'alice@test.com' " +
                    "WHERE NOT EXISTS (SELECT 1 FROM mydb.users)");

            System.out.println("Database, user, and table initialized successfully!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
