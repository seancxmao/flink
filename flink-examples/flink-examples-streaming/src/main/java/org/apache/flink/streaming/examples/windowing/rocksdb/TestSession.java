package org.apache.flink.streaming.examples.windowing.rocksdb;

public class TestSession {

  private byte[] data;

  public byte[] getData() {
    return data;
  }

  public void setData(byte[] data) {
    this.data = data;
  }
}
