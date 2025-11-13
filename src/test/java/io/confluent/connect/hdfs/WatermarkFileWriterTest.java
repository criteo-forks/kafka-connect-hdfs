package io.confluent.connect.hdfs;

import com.criteo.connect.hdfs.WatermarkFileWriter;
import com.criteo.connect.hdfs.WatermarkUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.hdfs.storage.HdfsStorage;
import io.confluent.connect.storage.StorageFactory;
import io.confluent.connect.storage.common.StorageCommonConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * Tests for {@link WatermarkFileWriter} using MiniDFS cluster helpers.
 */
public class WatermarkFileWriterTest extends TestWithMiniDFSCluster {

  private HdfsStorage storage;
  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  protected Map<String, String> createProps() {
    return super.createProps();
  }

  /** Initialize storage & config (invoked manually inside each test to allow custom props). */
  private void init() throws Exception {
    setUp(); // from HdfsSinkConnectorTestBase (not annotated to allow per-test customization)
    @SuppressWarnings("unchecked")
    Class<? extends HdfsStorage> storageClass = (Class<? extends HdfsStorage>)
        connectorConfig.getClass(StorageCommonConfig.STORAGE_CLASS_CONFIG);
    storage = StorageFactory.createStorage(
        storageClass,
        HdfsSinkConnectorConfig.class,
        connectorConfig,
        url
    );
  }

  @Test
  public void testWriteIfNewerBehavior() throws Exception {
    init();
    TopicPartition tp = TOPIC_PARTITION; // from base
    String topicDir = topicsDir.get(tp.topic());
    WatermarkFileWriter writer = new WatermarkFileWriter(storage, topicDir);

    String env = "test_env";
    String region = "test_dc";
    String cluster = "test_cluster";

    long ts1 = 1_000_000_000L; // epoch seconds
    WatermarkUtils.WatermarkMetadata meta1 = new WatermarkUtils.WatermarkMetadata(
        "com.criteo.glup.watermark",
        "host",
        tp.topic(),
        tp.partition(),
        1,
        "uuid",
        region,
        ts1,
        cluster,
        env,
        "all_sources",
        WatermarkUtils.WatermarkFormat.INJECTED
    );
    boolean firstWrite = writer.writeIfNewer(meta1);
    assertTrue("First write should create watermark file", firstWrite);

    String watermarkPath = storage.url() + "/" + topicDir + "/" + tp.topic() + "/watermarks/" + tp.topic() + "-" + tp.partition() + "-" + env + "-" + region + "-" + cluster + ".json";
    assertTrue(storage.exists(watermarkPath));
    byte[] data1 = storage.readBytes(watermarkPath);
    WatermarkFileWriter.WritableWatermark wm1 = mapper.readValue(data1, WatermarkFileWriter.WritableWatermark.class);
    assertEquals(ts1, wm1.timestamp);
    assertEquals(Integer.valueOf(1), wm1.partitionCount);

    // Older timestamp metadata
    WatermarkUtils.WatermarkMetadata olderMeta = new WatermarkUtils.WatermarkMetadata(
        "com.criteo.glup.watermark",
        "host",
        tp.topic(),
        tp.partition(),
        1,
        "uuid",
        region,
        ts1 - 10,
        cluster,
        env,
        "all_sources",
        WatermarkUtils.WatermarkFormat.INJECTED
    );
    assertFalse(writer.writeIfNewer(olderMeta));
    byte[] dataStill = storage.readBytes(watermarkPath);
    assertArrayEquals(data1, dataStill);

    // Newer timestamp metadata
    WatermarkUtils.WatermarkMetadata newerMeta = new WatermarkUtils.WatermarkMetadata(
        "com.criteo.glup.watermark",
        "host",
        tp.topic(),
        tp.partition(),
        1,
        "uuid",
        region,
        ts1 + 5,
        cluster,
        env,
        "all_sources",
        WatermarkUtils.WatermarkFormat.INJECTED
    );
    assertTrue(writer.writeIfNewer(newerMeta));
    byte[] data2 = storage.readBytes(watermarkPath);
    WatermarkFileWriter.WritableWatermark wm2 = mapper.readValue(data2, WatermarkFileWriter.WritableWatermark.class);
    assertEquals(ts1 + 5, wm2.timestamp);
    assertEquals(Integer.valueOf(1), wm2.partitionCount);
  }

  @Test
  public void testWriteIfNewerIdempotentSameTimestamp() throws Exception {
    init();
    TopicPartition tp = TOPIC_PARTITION;
    String topicDir = topicsDir.get(tp.topic());
    WatermarkFileWriter writer = new WatermarkFileWriter(storage, topicDir);
    String env = "test_env";
    String region = "test_dc";
    String cluster = "test_cluster";
    long ts = 2_000_000_000L;

    WatermarkUtils.WatermarkMetadata meta = new WatermarkUtils.WatermarkMetadata(
        "com.criteo.glup.watermark",
        "host",
        tp.topic(),
        tp.partition(),
        1,
        "uuid",
        region,
        ts,
        cluster,
        env,
        "all_sources",
        WatermarkUtils.WatermarkFormat.INJECTED
    );
    assertTrue(writer.writeIfNewer(meta));
    String watermarkPath = storage.url() + "/" + topicDir + "/" + tp.topic() + "/watermarks/" + tp.topic() + "-" + tp.partition() + "-" + env + "-" + region + "-" + cluster + ".json";
    byte[] first = storage.readBytes(watermarkPath);
    assertNotNull(first);

    // Same timestamp again
    WatermarkUtils.WatermarkMetadata sameMeta = new WatermarkUtils.WatermarkMetadata(
        "com.criteo.glup.watermark",
        "host",
        tp.topic(),
        tp.partition(),
        1,
        "uuid",
        region,
        ts,
        cluster,
        env,
        "all_sources",
        WatermarkUtils.WatermarkFormat.INJECTED
    );
    assertFalse(writer.writeIfNewer(sameMeta));
    byte[] second = storage.readBytes(watermarkPath);
    assertArrayEquals(first, second);
  }

  @Test
  public void testFirstWriteWithZeroAndNegativeTimestamp() throws Exception {
    init();
    TopicPartition tp = TOPIC_PARTITION;
    String topicDir = topicsDir.get(tp.topic());
    WatermarkFileWriter writer = new WatermarkFileWriter(storage, topicDir);
    String env = "test_env";
    String region = "test_dc";
    String cluster = "test_cluster";

    String watermarkPath = storage.url() + "/" + topicDir + "/" + tp.topic() + "/watermarks/" + tp.topic() + "-" + tp.partition() + "-" + env + "-" + region + "-" + cluster + ".json";

    WatermarkUtils.WatermarkMetadata zeroMeta = new WatermarkUtils.WatermarkMetadata(
        "com.criteo.glup.watermark","host",tp.topic(),tp.partition(),1,"uuid",region,0L,cluster,env,"all_sources",WatermarkUtils.WatermarkFormat.INJECTED);
    assertTrue(writer.writeIfNewer(zeroMeta));
    long existingZero = mapper.readValue(storage.readBytes(watermarkPath), WatermarkFileWriter.WritableWatermark.class).timestamp;
    assertEquals(0L, existingZero);

    storage.delete(watermarkPath);
    assertFalse(storage.exists(watermarkPath));

    WatermarkUtils.WatermarkMetadata negMeta = new WatermarkUtils.WatermarkMetadata(
        "com.criteo.glup.watermark","host",tp.topic(),tp.partition(),1,"uuid",region,-5L,cluster,env,"all_sources",WatermarkUtils.WatermarkFormat.INJECTED);
    assertTrue(writer.writeIfNewer(negMeta));
    long existingNeg = mapper.readValue(storage.readBytes(watermarkPath), WatermarkFileWriter.WritableWatermark.class).timestamp;
    assertEquals(-5L, existingNeg);
  }

  @Test
  public void testWriteUsingMetadataOverload() throws Exception {
    init();
    TopicPartition tp = TOPIC_PARTITION;
    String topicDir = topicsDir.get(tp.topic());
    WatermarkFileWriter writer = new WatermarkFileWriter(storage, topicDir);

    WatermarkUtils.WatermarkMetadata meta = new WatermarkUtils.WatermarkMetadata(
        "com.criteo.glup.watermark","h",tp.topic(),tp.partition(),1,"u","eu",1234567890L,"stream","prod","all_sources",WatermarkUtils.WatermarkFormat.INJECTED);
    assertTrue(writer.writeIfNewer(meta));
    String watermarkPath = storage.url() + "/" + topicDir + "/" + tp.topic() + "/watermarks/" + tp.topic() + "-" + tp.partition() + "-prod-eu-stream.json";
    byte[] data = storage.readBytes(watermarkPath);
    WatermarkFileWriter.WritableWatermark wm = mapper.readValue(data, WatermarkFileWriter.WritableWatermark.class);
    assertEquals(1234567890L, wm.timestamp);
  }
}
