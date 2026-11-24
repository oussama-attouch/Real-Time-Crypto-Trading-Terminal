import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Reads the dead-letter topic and prints, for every record:
 * the raw payload (as text, NOT base64) and all headers — especially the
 * {@code kafka_dlt-*} set that DeadLetterPublishingRecoverer adds.
 */
public class DltReader {

    private static final int MAX_HEADER_CHARS = 160;

    public static void main(String[] args) {
        String topic = args.length > 0 ? args[0] : "binance-raw-trades.DLT";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "dlt-verify-" + System.currentTimeMillis());
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");   // read the DLT from the start
        props.put("enable.auto.commit", "false");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            var partitions = consumer.partitionsFor(topic, Duration.ofSeconds(20));
            System.out.println("topic '" + topic + "' partitions = "
                    + (partitions == null ? 0 : partitions.size()));

            consumer.subscribe(List.of(topic));

            int seen = 0;
            long deadline = System.currentTimeMillis() + 25_000;
            while (seen < 3 && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(1000))) {
                    seen++;
                    print(record, seen);
                    if (seen >= 3) {
                        break;
                    }
                }
            }
            System.out.println();
            System.out.println("DLT records read = " + seen
                    + (seen == 0 ? "   <-- expected only if the DLT truly never received a record" : ""));
        }
    }

    private static void print(ConsumerRecord<String, byte[]> record, int index) {
        String payload = record.value() == null ? "<null>" : new String(record.value(), StandardCharsets.UTF_8);

        System.out.println();
        System.out.println("========== DLT record #" + index + " ==========");
        System.out.println("key        = " + record.key());
        System.out.println("partition  = " + record.partition());
        System.out.println("offset     = " + record.offset());
        System.out.println("valueBytes = " + (record.value() == null ? 0 : record.value().length));
        System.out.println("payload    = " + payload);

        boolean rawJson = payload.startsWith("{") || payload.startsWith("[");
        boolean looksBase64 = payload.matches("^[A-Za-z0-9+/]{20,}={0,2}$");
        System.out.println("payloadIs  = " + (record.value() == null ? "NULL"
                : looksBase64 ? "BASE64  <-- warning: raw bytes were re-encoded"
                : rawJson ? "RAW JSON/text  (expected)"
                : "opaque text"));

        List<Header> headers = new ArrayList<>();
        record.headers().forEach(headers::add);
        headers.sort((a, b) -> a.key().compareTo(b.key()));

        System.out.println("headers    = " + headers.size());
        for (Header header : headers) {
            String value = header.value() == null ? "<null>"
                    : (header.key().equals("kafka_dlt-exception-stacktrace")
                        ? "<" + header.value().length + " bytes of stack trace>"
                        : abbreviate(new String(header.value(), StandardCharsets.UTF_8)));
            String marker = header.key().startsWith("kafka_dlt-") ? " *" : "";
            System.out.println("   " + header.key() + " = " + value + marker);
        }
        System.out.println("   (* = added by DeadLetterPublishingRecoverer)");
    }

    private static String abbreviate(String s) {
        String oneLine = s.replace("\r", " ").replace("\n", " ");
        return oneLine.length() <= MAX_HEADER_CHARS
                ? oneLine
                : oneLine.substring(0, MAX_HEADER_CHARS) + "...";
    }
}
