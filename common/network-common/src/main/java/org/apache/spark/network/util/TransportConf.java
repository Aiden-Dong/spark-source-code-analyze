/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.util;

import java.util.Locale;
import java.util.Properties;

import com.google.common.primitives.Ints;

/**
 * A central location that tracks all the settings we expose to users.
 */
public class TransportConf {

  private final String SPARK_NETWORK_IO_MODE_KEY;
  private final String SPARK_NETWORK_IO_PREFERDIRECTBUFS_KEY;
  private final String SPARK_NETWORK_IO_CONNECTIONTIMEOUT_KEY;
  private final String SPARK_NETWORK_IO_BACKLOG_KEY;
  private final String SPARK_NETWORK_IO_NUMCONNECTIONSPERPEER_KEY;
  private final String SPARK_NETWORK_IO_SERVERTHREADS_KEY;
  private final String SPARK_NETWORK_IO_CLIENTTHREADS_KEY;
  private final String SPARK_NETWORK_IO_RECEIVEBUFFER_KEY;
  private final String SPARK_NETWORK_IO_SENDBUFFER_KEY;
  private final String SPARK_NETWORK_SASL_TIMEOUT_KEY;
  private final String SPARK_NETWORK_IO_MAXRETRIES_KEY;
  private final String SPARK_NETWORK_IO_RETRYWAIT_KEY;
  private final String SPARK_NETWORK_IO_LAZYFD_KEY;
  private final String SPARK_NETWORK_VERBOSE_METRICS;

  private final ConfigProvider conf;

  private final String module;

  public TransportConf(String module, ConfigProvider conf) {
    this.module = module;
    this.conf = conf;
    SPARK_NETWORK_IO_MODE_KEY = getConfKey("io.mode");
    SPARK_NETWORK_IO_PREFERDIRECTBUFS_KEY = getConfKey("io.preferDirectBufs");
    SPARK_NETWORK_IO_CONNECTIONTIMEOUT_KEY = getConfKey("io.connectionTimeout");
    SPARK_NETWORK_IO_BACKLOG_KEY = getConfKey("io.backLog");
    SPARK_NETWORK_IO_NUMCONNECTIONSPERPEER_KEY =  getConfKey("io.numConnectionsPerPeer");
    SPARK_NETWORK_IO_SERVERTHREADS_KEY = getConfKey("io.serverThreads");
    SPARK_NETWORK_IO_CLIENTTHREADS_KEY = getConfKey("io.clientThreads");
    SPARK_NETWORK_IO_RECEIVEBUFFER_KEY = getConfKey("io.receiveBuffer");
    SPARK_NETWORK_IO_SENDBUFFER_KEY = getConfKey("io.sendBuffer");
    SPARK_NETWORK_SASL_TIMEOUT_KEY = getConfKey("sasl.timeout");
    SPARK_NETWORK_IO_MAXRETRIES_KEY = getConfKey("io.maxRetries");
    SPARK_NETWORK_IO_RETRYWAIT_KEY = getConfKey("io.retryWait");
    SPARK_NETWORK_IO_LAZYFD_KEY = getConfKey("io.lazyFD");
    SPARK_NETWORK_VERBOSE_METRICS = getConfKey("io.enableVerboseMetrics");
  }

  public int getInt(String name, int defaultValue) {
    return conf.getInt(name, defaultValue);
  }

  public String get(String name, String defaultValue) {
    return conf.get(name, defaultValue);
  }

  private String getConfKey(String suffix) {
    return "spark." + module + "." + suffix;
  }

  public String getModuleName() {
    return module;
  }

  /** IO mode: nio or epoll */
  public String ioMode() {
    return conf.get(SPARK_NETWORK_IO_MODE_KEY, "NIO").toUpperCase(Locale.ROOT);
  }

  /** If true, we will prefer allocating off-heap byte buffers within Netty. */
  public boolean preferDirectBufs() {
    return conf.getBoolean(SPARK_NETWORK_IO_PREFERDIRECTBUFS_KEY, true);
  }

  /** Connect timeout in milliseconds. Default 120 secs. */
  public int connectionTimeoutMs() {
    long defaultNetworkTimeoutS = JavaUtils.timeStringAsSec(
      conf.get("spark.network.timeout", "120s"));
    long defaultTimeoutMs = JavaUtils.timeStringAsSec(
      conf.get(SPARK_NETWORK_IO_CONNECTIONTIMEOUT_KEY, defaultNetworkTimeoutS + "s")) * 1000;
    return (int) defaultTimeoutMs;
  }

  /** Number of concurrent connections between two nodes for fetching data. */
  public int numConnectionsPerPeer() {
    return conf.getInt(SPARK_NETWORK_IO_NUMCONNECTIONSPERPEER_KEY, 1);
  }

  /** Requested maximum length of the queue of incoming connections. Default -1 for no backlog. */
  public int backLog() { return conf.getInt(SPARK_NETWORK_IO_BACKLOG_KEY, -1); }

  /** Number of threads used in the server thread pool. Default to 0, which is 2x#cores. */
  public int serverThreads() { return conf.getInt(SPARK_NETWORK_IO_SERVERTHREADS_KEY, 0); }

  /** Number of threads used in the client thread pool. Default to 0, which is 2x#cores. */
  public int clientThreads() { return conf.getInt(SPARK_NETWORK_IO_CLIENTTHREADS_KEY, 0); }

  /**
   * Receive buffer size (SO_RCVBUF).
   * Note: the optimal size for receive buffer and send buffer should be
   *  latency * network_bandwidth.
   * Assuming latency = 1ms, network_bandwidth = 10Gbps
   *  buffer size should be ~ 1.25MB
   */
  public int receiveBuf() { return conf.getInt(SPARK_NETWORK_IO_RECEIVEBUFFER_KEY, -1); }

  /** Send buffer size (SO_SNDBUF). */
  public int sendBuf() { return conf.getInt(SPARK_NETWORK_IO_SENDBUFFER_KEY, -1); }

  /** Timeout for a single round trip of auth message exchange, in milliseconds. */
  public int authRTTimeoutMs() {
    return (int) JavaUtils.timeStringAsSec(conf.get("spark.network.auth.rpcTimeout",
      conf.get(SPARK_NETWORK_SASL_TIMEOUT_KEY, "30s"))) * 1000;
  }

  /**
   * Max number of times we will try IO exceptions (such as connection timeouts) per request.
   * If set to 0, we will not do any retries.
   */
  public int maxIORetries() { return conf.getInt(SPARK_NETWORK_IO_MAXRETRIES_KEY, 3); }

  /**
   * Time (in milliseconds) that we will wait in order to perform a retry after an IOException.
   * Only relevant if maxIORetries &gt; 0.
   */
  public int ioRetryWaitTimeMs() {
    return (int) JavaUtils.timeStringAsSec(conf.get(SPARK_NETWORK_IO_RETRYWAIT_KEY, "5s")) * 1000;
  }

  /**
   * Minimum size of a block that we should start using memory map rather than reading in through
   * normal IO operations. This prevents Spark from memory mapping very small blocks. In general,
   * memory mapping has high overhead for blocks close to or below the page size of the OS.
   */
  public int memoryMapBytes() {
    return Ints.checkedCast(JavaUtils.byteStringAsBytes(
      conf.get("spark.storage.memoryMapThreshold", "2m")));
  }

  /**
   * Whether to initialize FileDescriptor lazily or not. If true, file descriptors are
   * created only when data is going to be transferred. This can reduce the number of open files.
   */
  public boolean lazyFileDescriptor() {
    return conf.getBoolean(SPARK_NETWORK_IO_LAZYFD_KEY, true);
  }

  /**
   * Whether to track Netty memory detailed metrics. If true, the detailed metrics of Netty
   * PoolByteBufAllocator will be gotten, otherwise only general memory usage will be tracked.
   */
  public boolean verboseMetrics() {
    return conf.getBoolean(SPARK_NETWORK_VERBOSE_METRICS, false);
  }

  /**
   * Maximum number of retries when binding to a port before giving up.
   */
  public int portMaxRetries() {
    return conf.getInt("spark.port.maxRetries", 16);
  }

  /**
   * Enables strong encryption. Also enables the new auth protocol, used to negotiate keys.
   */
  public boolean encryptionEnabled() {
    return conf.getBoolean("spark.network.crypto.enabled", false);
  }

  /**
   * The cipher transformation to use for encrypting session data.
   */
  public String cipherTransformation() {
    return conf.get("spark.network.crypto.cipher", "AES/CTR/NoPadding");
  }

  /**
   * The key generation algorithm. This should be an algorithm that accepts a "PBEKeySpec"
   * as input. The default value (PBKDF2WithHmacSHA1) is available in Java 7.
   */
  public String keyFactoryAlgorithm() {
    return conf.get("spark.network.crypto.keyFactoryAlgorithm", "PBKDF2WithHmacSHA1");
  }

  /**
   * How many iterations to run when generating keys.
   *
   * See some discussion about this at: http://security.stackexchange.com/q/3959
   * The default value was picked for speed, since it assumes that the secret has good entropy
   * (128 bits by default), which is not generally the case with user passwords.
   */
  public int keyFactoryIterations() {
    return conf.getInt("spark.network.crypto.keyFactoryIterations", 1024);
  }

  /**
   * Encryption key length, in bits.
   */
  public int encryptionKeyLength() {
    return conf.getInt("spark.network.crypto.keyLength", 128);
  }

  /**
   * Initial vector length, in bytes.
   */
  public int ivLength() {
    return conf.getInt("spark.network.crypto.ivLength", 16);
  }

  /**
   * The algorithm for generated secret keys. Nobody should really need to change this,
   * but configurable just in case.
   */
  public String keyAlgorithm() {
    return conf.get("spark.network.crypto.keyAlgorithm", "AES");
  }

  /**
   * Whether to fall back to SASL if the new auth protocol fails. Enabled by default for
   * backwards compatibility.
   */
  public boolean saslFallback() {
    return conf.getBoolean("spark.network.crypto.saslFallback", true);
  }

  /**
   * Whether to enable SASL-based encryption when authenticating using SASL.
   */
  public boolean saslEncryption() {
    return conf.getBoolean("spark.authenticate.enableSaslEncryption", false);
  }

  /**
   * Maximum number of bytes to be encrypted at a time when SASL encryption is used.
   */
  public int maxSaslEncryptedBlockSize() {
    return Ints.checkedCast(JavaUtils.byteStringAsBytes(
      conf.get("spark.network.sasl.maxEncryptedBlockSize", "64k")));
  }

  /**
   * Whether the server should enforce encryption on SASL-authenticated connections.
   */
  public boolean saslServerAlwaysEncrypt() {
    return conf.getBoolean("spark.network.sasl.serverAlwaysEncrypt", false);
  }

  /**
   * The commons-crypto configuration for the module.
   */
  public Properties cryptoConf() {
    return CryptoUtils.toCryptoConf("spark.network.crypto.config.", conf.getAll());
  }

  /**
   * The max number of chunks allowed to be transferred at the same time on shuffle service.
   * Note that new incoming connections will be closed when the max number is hit. The client will
   * retry according to the shuffle retry configs (see `spark.shuffle.io.maxRetries` and
   * `spark.shuffle.io.retryWait`), if those limits are reached the task will fail with fetch
   * failure.
   */
  public long maxChunksBeingTransferred() {
    return conf.getLong("spark.shuffle.maxChunksBeingTransferred", Long.MAX_VALUE);
  }
}
