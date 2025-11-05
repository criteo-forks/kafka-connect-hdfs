package com.criteo.connect.hdfs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public class WatermarkUtils {
  private static final Logger log = LoggerFactory.getLogger(WatermarkUtils.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Flat representation of a watermark metadata entry.
   */
  public static final class WatermarkMetadata {
    public final String type;
    public final String hostname;
    public final String kafkaTopic;
    public final Integer partition;
    public final Integer partitionCount;
    public final String processUuid;
    public final String region;
    /** Epoch seconds (as provided in input). */
    public final Long timestamp;
    public final String cluster;
    public final String environment;
    public final String consensusType;
    public final WatermarkFormat watermarkFormat;

    public WatermarkMetadata(
        String type,
        String hostname,
        String kafkaTopic,
        Integer partition,
        Integer partitionCount,
        String processUuid,
        String region,
        Long timestamp,
        String cluster,
        String environment,
        String consensusType,
        WatermarkFormat watermarkFormat
    ) {
      this.type = type;
      this.hostname = hostname;
      this.kafkaTopic = kafkaTopic;
      this.partition = partition;
      this.partitionCount = partitionCount;
      this.processUuid = processUuid;
      this.region = region;
      this.timestamp = timestamp;
      this.cluster = cluster;
      this.environment = environment;
      this.consensusType = consensusType;
      this.watermarkFormat = watermarkFormat;
    }

    @Override
    public String toString() {
      return "WatermarkMetadata{"
          + "type='" + type + '\''
          + ", hostname='" + hostname + '\''
          + ", kafkaTopic='" + kafkaTopic + '\''
          + ", partition=" + partition
          + ", partitionCount=" + partitionCount
          + ", processUuid='" + processUuid + '\''
          + ", region='" + region + '\''
          + ", timestamp=" + timestamp
          + ", cluster='" + cluster + '\''
          + ", environment='" + environment + '\''
          + ", consensusType='" + consensusType + '\''
          + ", sourceFormat=" + watermarkFormat
          + '}';
    }

    public boolean isWrapped() {
      return watermarkFormat == WatermarkFormat.REINJECTED;
    }

    /**
     * Human-readable ISO-8601 representation of the watermark timestamp in UTC.
     * Returns null if timestamp is null.
     */
    public String humanTimestamp() {
      return WatermarkUtils.formatTimestamp(timestamp);
    }
  }

  public enum WatermarkFormat { REINJECTED, INJECTED }

  // Primary watermark key used in Kafka message keys
  public static final String WATERMARK_KEY = "com.criteo.kafka.watermarks";

  /**
   * Decode a watermark JSON payload into a {@link WatermarkMetadata} instance.
   * Returns null if the structure is invalid or required metadata array is missing.
   *
   * <p>Expected structure:
   * {"__metadata": [{"type": "com.criteo.glup.watermark", ... }]}
   *
   * <p>Only the first element of the __metadata array is considered.
   */
  public static WatermarkMetadata decodeWatermark(String json) {
    if (json == null || json.isEmpty()) {
      return null;
    }
    try {
      JsonNode root = MAPPER.readTree(json);

      // Try wrapped format first
      JsonNode arr = root.get("__metadata");
      if (arr != null && arr.isArray() && !arr.isEmpty()) {
        WatermarkMetadata wm = buildFromNode(arr.get(0), WatermarkFormat.REINJECTED);
        if (wm != null) {
          return wm;
        }
      }
      // Fallback to flat format if "type" at root
      WatermarkMetadata flat = buildFromNode(root, WatermarkFormat.INJECTED);
      return flat;
    } catch (IOException e) {
      log.debug("Failed to parse watermark json", e);
      return null;
    }
  }

  /** Convenience overload for byte[] payloads. */
  public static WatermarkMetadata decodeWatermark(byte[] jsonBytes) {
    if (jsonBytes == null || jsonBytes.length == 0) {
      return null;
    }
    return decodeWatermark(new String(jsonBytes));
  }

  /** Quick predicate to check if a JSON string looks like a watermark message. */
  public static boolean isWatermarkJson(String json) {
    WatermarkMetadata md = decodeWatermark(json);
    return md != null;
  }

  /** Determine if a SinkRecord is a watermark message based on its key. */
  public static boolean isWatermark(SinkRecord record) {
    try {
      Object key = record.key();
      if (key instanceof String) {
        return WATERMARK_KEY.equals(key);
      } else if (key instanceof byte[]) {
        return WATERMARK_KEY.equals(new String((byte[]) key));
      }
      return false;
    } catch (Exception e) {
      return false;
    }
  }

  private static final DateTimeFormatter ISO_UTC = DateTimeFormatter.ISO_INSTANT;

  /** Format an epoch seconds value into ISO-8601 UTC. Returns null if input null. */
  public static String formatTimestamp(Long epochSeconds) {
    if (epochSeconds == null) {
      return null;
    }
    try {
      return ISO_UTC.format(Instant.ofEpochSecond(epochSeconds).atZone(ZoneOffset.UTC));
    } catch (Exception e) {
      log.debug("Failed to format timestamp {}", epochSeconds, e);
      return null;
    }
  }

  private static String text(JsonNode node, String field) {
    JsonNode f = node.get(field);
    return f != null && !f.isNull() ? f.asText() : null;
  }

  private static Integer intValue(JsonNode node, String field) {
    JsonNode f = node.get(field);
    return f != null && f.isInt() ? f.asInt() : (f != null && f.isNumber() ? f.asInt() : null);
  }

  private static Long longValue(JsonNode node, String field) {
    JsonNode f = node.get(field);
    return f != null && f.isNumber() ? f.asLong() : null;
  }

  /** Simple equality helper if needed elsewhere */
  public static boolean equals(WatermarkMetadata a, WatermarkMetadata b) {
    return a == b || (a != null && b != null && Objects.equals(a.toString(), b.toString()));
  }

  private static WatermarkMetadata buildFromNode(JsonNode node, WatermarkFormat format) {
    if (node == null || node.isNull()) {
      return null;
    }
    String type = text(node, "type");
    if (type == null || !type.contains("watermark")) {
      return null;
    }
    String hostname = text(node, "hostname");
    String kafkaTopic = text(node, "kafka_topic");
    Integer partition = intValue(node, "partition");
    Integer partitionCount = intValue(node, "partition_count");
    String processUuid = text(node, "process_uuid");
    String region = text(node, "region");
    Long timestamp = longValue(node, "timestamp");
    String cluster = text(node, "cluster");
    String environment = text(node, "environment");
    String consensusType = text(node, "consensus_type");
    return new WatermarkMetadata(
        type,
        hostname,
        kafkaTopic,
        partition,
        partitionCount,
        processUuid,
        region,
        timestamp,
        cluster,
        environment,
        consensusType,
        format
    );
  }
}
