package org.apache.flink.streaming.examples.windowing.rocksdb;

public class TestAccumulator {

  private TestSession session;

  public TestAccumulator() {
	  session = new TestSession();
  }

  public TestSession getSession() {
    return session;
  }

  public void setSession(TestSession session) {
    this.session = session;
  }
}
