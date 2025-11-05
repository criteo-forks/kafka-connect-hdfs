package com.criteo.connect.hdfs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;

/**
 * Utility responsible for writing a watermark file for a topic partition.
 * The file is only updated when the new timestamp is strictly greater
 * than the one currently stored.
 */
public class WatermarkFileWriter {
  private static final Logger log = LoggerFactory.getLogger(WatermarkFileWriter.class);
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

  private final HdfsStorage storage;
  private final String topicsDir;

  public WatermarkFileWriter(HdfsStorage storage, String topicsDir) {
    this.storage = storage;
    this.topicsDir = topicsDir;
  }

  /** Code representation of a watermark file contents. */
  public static final class WritableWatermark {
    public final String topic;
    public final int partition;
    public final Integer partitionCount;
    /** Epoch seconds */
    public final long timestamp;
    public final String humanTimestamp;

    @JsonCreator
    public WritableWatermark(
        @JsonProperty("topic") String topic,
        @JsonProperty("partition") int partition,
        @JsonProperty("partitionCount") Integer partitionCount,
        @JsonProperty("timestamp") long timestamp,
        @JsonProperty("humanTimestamp") String humanTimestamp
    ) {
      this.topic = topic;
      this.partition = partition;
      this.partitionCount = partitionCount;
      this.timestamp = timestamp;
      this.humanTimestamp = humanTimestamp;
    }

    public static WritableWatermark of(TopicPartition tp, long epochSeconds,
                                       Integer partitionCount) {
      String human = WatermarkUtils.formatTimestamp(epochSeconds);
      if (human == null) {
        human = Long.toString(epochSeconds);
      }
      return new WritableWatermark(tp.topic(), tp.partition(), partitionCount, epochSeconds, human);
    }
  }

  /**
   * Build full HDFS watermark file path for given metadata, or null if insufficient info.
   */
  public String watermarkFilePath(WatermarkUtils.WatermarkMetadata meta) {
    if (meta == null || meta.kafkaTopic == null || meta.partition == null) {
      return null;
    }
    String env = meta.environment == null ? "unknown-env" : meta.environment;
    String region = meta.region == null ? "unknown-region" : meta.region;
    String cluster = meta.cluster == null ? "unknown-cluster" : meta.cluster;
    TopicPartition tp = new TopicPartition(meta.kafkaTopic, meta.partition);
    return watermarkDirectory(meta.kafkaTopic) + "/" + watermarkFileName(tp, env, region, cluster);
  }

  /** Normalize timestamp, environment, region, cluster from metadata. */
  private long extractTimestamp(WatermarkUtils.WatermarkMetadata meta) {
    return meta.timestamp == null ? -1L : meta.timestamp;
  }

  /**
   * Convenience overload: write watermark using a WatermarkMetadata instance.
   * Uses helper watermarkFilePath for path building.
   */
  public boolean writeIfNewer(WatermarkUtils.WatermarkMetadata meta) {
    String filePath = watermarkFilePath(meta);
    if (filePath == null) {
      return false;
    }
    TopicPartition tp = new TopicPartition(meta.kafkaTopic, meta.partition);
    long ts = extractTimestamp(meta);
    long existingTs = readExistingTimestamp(filePath);
    String directory = watermarkDirectory(meta.kafkaTopic);
    if (existingTs < 0) {
      return writeFile(tp, ts, existingTs, filePath, directory, meta.partitionCount);
    }
    if (ts <= existingTs) {
      if (log.isDebugEnabled()) {
        log.debug("Skipping watermark update for {} because {} <= existing {}", tp, ts, existingTs);
      }
      return false;
    }
    return writeFile(tp, ts, existingTs, filePath, directory, meta.partitionCount);
  }

  private String watermarkDirectory(String topic) {
    return storage.url() + "/" + topicsDir + "/" + topic + "/watermarks";
  }

  private String watermarkFileName(TopicPartition tp, String environment,
                                   String region, String cluster) {
    return tp.topic() + "-" + tp.partition() + "-"
            + environment + "-" + region + "-" + cluster + ".json";
  }

  private boolean writeFile(TopicPartition tp, long epochSeconds, long previousTs,
                            String filePath, String directory, Integer partitionCount) {
    ensureDirectoryExists(directory);
    WritableWatermark wm = WritableWatermark.of(tp, epochSeconds, partitionCount);
    String tempFilePath = filePath + ".tmp." + System.currentTimeMillis();
    try (OutputStream os = storage.create(tempFilePath, true)) {
      String json = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(wm) + "\n";
      os.write(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      os.flush();
    } catch (ConnectException e) {
      throw e;
    } catch (Exception e) {
      throw new ConnectException("Failed to write temp watermark file " + tempFilePath, e);
    }
    try {
      storage.renameFileWithOverride(tempFilePath, filePath);
      log.info("Updated watermark file {} for {} with timestamp {} (prev {})",
              filePath, tp, epochSeconds, previousTs);
      return true;
    } catch (Exception e) {
      throw new ConnectException("Failed to atomically rename watermark file to " + filePath, e);
    }
  }

  private void ensureDirectoryExists(String dirPath) {
    if (!storage.exists(dirPath)) {
      storage.create(dirPath);
    }
  }

  /**
   * Reads existing watermark file and extracts numeric timestamp.
   * @param filePath path to watermark file
   * @return existing timestamp in epoch seconds, or -1 if file missing/unparseable
   */
  private long readExistingTimestamp(String filePath) {
    if (!storage.exists(filePath)) {
      return -1L;
    }
    try {
      byte[] data = storage.readBytes(filePath);
      if (data == null) {
        return -1L;
      }
      WritableWatermark wm = MAPPER.readValue(data, WritableWatermark.class);
      return wm.timestamp;
    } catch (Exception e) {
      log.warn("Failed to read existing watermark file {}: {}", filePath, e.getMessage());
      return -1L;
    }
  }
}
