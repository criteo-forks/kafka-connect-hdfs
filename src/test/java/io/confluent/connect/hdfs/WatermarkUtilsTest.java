package io.confluent.connect.hdfs;

import com.criteo.connect.hdfs.WatermarkUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class WatermarkUtilsTest {

  private static final String WRAPPED_JSON = "{" +
      "\"__metadata\": [{" +
      "\"type\": \"com.criteo.glup.watermark\"," +
      "\"hostname\": \"host1\"," +
      "\"kafka_topic\": \"topicA\"," +
      "\"partition\": 2," +
      "\"partition_count\": 10," +
      "\"process_uuid\": \"uuid-123\"," +
      "\"region\": \"eu\"," +
      "\"timestamp\": 1761368592," +
      "\"cluster\": \"stream\"," +
      "\"environment\": \"prod\"," +
      "\"consensus_type\": \"all_sources\"}]" +
      "}";

  private static final String FLAT_JSON = "{" +
      "\"type\": \"com.criteo.glup.watermark\"," +
      "\"hostname\": \"host2\"," +
      "\"kafka_topic\": \"topicB\"," +
      "\"partition\": 3," +
      "\"partition_count\": 5," +
      "\"process_uuid\": \"uuid-456\"," +
      "\"region\": \"us\"," +
      "\"timestamp\": 1761368600," +
      "\"cluster\": \"stream\"," +
      "\"environment\": \"preprod\"," +
      "\"consensus_type\": \"all_sources\"}";

  @Test
  public void testDecodeWrappedWatermark() {
    WatermarkUtils.WatermarkMetadata meta = WatermarkUtils.decodeWatermark(WRAPPED_JSON);
    assertNotNull(meta);
    assertEquals(WatermarkUtils.WatermarkFormat.REINJECTED, meta.watermarkFormat);
    assertEquals("host1", meta.hostname);
    assertEquals("topicA", meta.kafkaTopic);
    assertEquals(Integer.valueOf(2), meta.partition);
    assertEquals(Integer.valueOf(10), meta.partitionCount);
    assertEquals(Long.valueOf(1761368592L), meta.timestamp);
    assertNotNull(meta.humanTimestamp());
    assertTrue(meta.humanTimestamp().contains("T"));
    assertTrue(meta.isWrapped());
  }

  @Test
  public void testDecodeFlatWatermark() {
    WatermarkUtils.WatermarkMetadata meta = WatermarkUtils.decodeWatermark(FLAT_JSON);
    assertNotNull(meta);
    assertEquals(WatermarkUtils.WatermarkFormat.INJECTED, meta.watermarkFormat);
    assertEquals("host2", meta.hostname);
    assertEquals("topicB", meta.kafkaTopic);
    assertEquals(Integer.valueOf(3), meta.partition);
    assertEquals(Integer.valueOf(5), meta.partitionCount);
    assertEquals(Long.valueOf(1761368600L), meta.timestamp);
    assertNotNull(meta.humanTimestamp());
    assertFalse(meta.isWrapped());
  }

  @Test
  public void testIsWatermarkJsonPredicate() {
    assertTrue(WatermarkUtils.isWatermarkJson(WRAPPED_JSON));
    assertTrue(WatermarkUtils.isWatermarkJson(FLAT_JSON));
    assertFalse(WatermarkUtils.isWatermarkJson("{\"foo\":1}"));
  }

  @Test
  public void testDecodeInvalidReturnsNull() {
    assertNull(WatermarkUtils.decodeWatermark((String) null));
    assertNull(WatermarkUtils.decodeWatermark(""));
    assertNull(WatermarkUtils.decodeWatermark("{\"notype\":1}"));
  }

  @Test
  public void testHumanTimestampNull() {
    assertNull(WatermarkUtils.formatTimestamp(null));
  }

  @Test
  public void testEquality() {
    WatermarkUtils.WatermarkMetadata a = WatermarkUtils.decodeWatermark(FLAT_JSON);
    WatermarkUtils.WatermarkMetadata b = WatermarkUtils.decodeWatermark(FLAT_JSON);
    assertTrue(WatermarkUtils.equals(a, b));
  }
}
