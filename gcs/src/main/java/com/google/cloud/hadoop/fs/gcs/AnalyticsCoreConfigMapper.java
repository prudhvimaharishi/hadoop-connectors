/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hadoop.fs.gcs;

import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.GCS_CONFIG_PREFIX;

import com.google.cloud.hadoop.util.HadoopConfigurationProperty;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration;
import com.google.cloud.hadoop.util.HadoopCredentialsConfiguration.AuthenticationType;
import com.google.cloud.hadoop.util.RedactedString;
import com.google.cloud.hadoop.util.RequesterPaysOptions.RequesterPaysMode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;

/** Maps GCS Hadoop Connector configurations to GCS Analytics Core configurations. */
final class AnalyticsCoreConfigMapper {

  static final String PROJECT_ID_KEY = "project-id";
  static final String USER_PROJECT_KEY = "user-project";
  static final String SERVICE_HOST_KEY = "service.host";
  static final String READ_THREAD_COUNT_KEY = "analytics-core.read.thread.count";
  static final String MAX_MERGE_GAP_KEY = "analytics-core.read.vectored.range.merge-gap.max-bytes";
  static final String MAX_MERGE_SIZE_KEY =
      "analytics-core.read.vectored.range.merged-size.max-bytes";
  static final String USER_AGENT_KEY = "user-agent";
  static final String FILE_ACCESS_PATTERN_KEY = "analytics-core.read.file-access-pattern";
  static final String INPLACE_SEEK_LIMIT_KEY = "analytics-core.read.inplace-seek-limit-bytes";
  static final String RANDOM_READ_MIN_REQ_SIZE_KEY = "analytics-core.random-read.min-request-size";
  static final String ADAPTIVE_READ_SEQ_THRESHOLD_KEY =
      "analytics-core.adaptive-read.sequential-read-threshold";
  static final String UPLOAD_CHUNK_SIZE_KEY = "channel.write.chunk-size-bytes";
  static final String UPLOAD_TYPE_KEY = "channel.write.upload-type";
  static final String TEMPORARY_PATHS_KEY = "channel.write.temporary-paths";
  static final String PCU_BUFFER_COUNT_KEY = "channel.write.pcu.buffer.count";
  static final String PCU_BUFFER_CAPACITY_KEY = "channel.write.pcu.buffer.capacity-bytes";
  static final String PCU_PART_FILE_CLEANUP_TYPE_KEY = "channel.write.pcu.part-file.cleanup-type";
  static final String PCU_PART_FILE_NAME_PREFIX_KEY = "channel.write.pcu.part-file.name-prefix";
  static final String ENCRYPTION_KEY_KEY = "encryption-key";
  static final String DECRYPTION_KEY_KEY = "decryption-key";
  static final String CHECKSUM_VALIDATION_ENABLED_KEY = "channel.write.checksum-validation.enabled";
  static final String HADOOP_UNIVERSE_DOMAIN_KEY = "fs.gs.universe.domain";
  static final String UNIVERSE_DOMAIN_KEY = "universe-domain";
  static final String AUTH_TYPE_KEY = "analytics-core.auth.type";
  static final String SERVICE_ACCOUNT_JSON_KEYFILE_KEY =
      "analytics-core.auth.service-account-json-keyfile";
  static final String WORKLOAD_IDENTITY_CREDENTIAL_CONFIG_FILE_KEY =
      "analytics-core.auth.workload-identity-federation.credential-config-file";
  static final String CLIENT_ID_KEY = "analytics-core.auth.client-id";
  static final String CLIENT_SECRET_KEY = "analytics-core.auth.client-secret";
  static final String REFRESH_TOKEN_KEY = "analytics-core.auth.refresh-token";
  static final String IMPERSONATION_SERVICE_ACCOUNT_KEY =
      "analytics-core.auth.impersonation-service-account";
  static final String TOKEN_SERVER_URI_KEY = "analytics-core.auth.token-server-uri";
  static final String PROXY_ADDRESS_KEY = "analytics-core.auth.proxy.address";
  static final String PROXY_USERNAME_KEY = "analytics-core.auth.proxy.username";
  static final String PROXY_PASSWORD_KEY = "analytics-core.auth.proxy.password";
  static final String CONNECT_TIMEOUT_KEY = "analytics-core.auth.http.connect-timeout-ms";
  static final String READ_TIMEOUT_KEY = "analytics-core.auth.http.read-timeout-ms";

  private static final String WORKLOAD_IDENTITY_FEDERATION_VALUE = "WORKLOAD_IDENTITY_FEDERATION";

  private static final ImmutableList<String> KEY_PREFIXES =
      ImmutableList.copyOf(HadoopCredentialsConfiguration.getConfigKeyPrefixes(GCS_CONFIG_PREFIX));

  /**
   * Connector-spelled auth and transport properties bound to {@code ["fs.gs", "google.cloud"]}.
   *
   * <p>Constructed as fresh instances rather than reusing the shared suffix constants in {@link
   * HadoopCredentialsConfiguration}, because {@code withPrefixes} mutates those shared instances.
   */
  private static final HadoopConfigurationProperty<AuthenticationType> AUTHENTICATION_TYPE =
      new HadoopConfigurationProperty<>(
              HadoopCredentialsConfiguration.AUTHENTICATION_TYPE_SUFFIX.getKey(),
              HadoopCredentialsConfiguration.AUTHENTICATION_TYPE_SUFFIX.getDefault())
          .withPrefixes(KEY_PREFIXES);

  private static final HadoopConfigurationProperty<String> SERVICE_ACCOUNT_JSON_KEYFILE =
      new HadoopConfigurationProperty<String>(
              HadoopCredentialsConfiguration.SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX.getKey())
          .withPrefixes(KEY_PREFIXES);

  private static final HadoopConfigurationProperty<String>
      WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE =
          new HadoopConfigurationProperty<String>(
                  HadoopCredentialsConfiguration
                      .WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE_SUFFIX
                      .getKey())
              .withPrefixes(KEY_PREFIXES);

  private static final HadoopConfigurationProperty<String> AUTH_CLIENT_ID =
      new HadoopConfigurationProperty<String>(
              HadoopCredentialsConfiguration.AUTH_CLIENT_ID_SUFFIX.getKey())
          .withPrefixes(KEY_PREFIXES);

  private static final HadoopConfigurationProperty<RedactedString> AUTH_CLIENT_SECRET =
      new HadoopConfigurationProperty<RedactedString>(
              HadoopCredentialsConfiguration.AUTH_CLIENT_SECRET_SUFFIX.getKey())
          .withPrefixes(KEY_PREFIXES);

  private static final HadoopConfigurationProperty<RedactedString> AUTH_REFRESH_TOKEN =
      new HadoopConfigurationProperty<RedactedString>(
              HadoopCredentialsConfiguration.AUTH_REFRESH_TOKEN_SUFFIX.getKey())
          .withPrefixes(KEY_PREFIXES);

  private static final HadoopConfigurationProperty<String> TOKEN_SERVER_URL =
      new HadoopConfigurationProperty<String>(
              HadoopCredentialsConfiguration.TOKEN_SERVER_URL_SUFFIX.getKey())
          .withPrefixes(KEY_PREFIXES);

  private static final HadoopConfigurationProperty<String> GCS_PROXY_ADDRESS =
      new HadoopConfigurationProperty<String>(
              HadoopCredentialsConfiguration.PROXY_ADDRESS_SUFFIX.getKey())
          .withPrefixes(KEY_PREFIXES);

  private static final HadoopConfigurationProperty<RedactedString> GCS_PROXY_USERNAME =
      new HadoopConfigurationProperty<RedactedString>(
              HadoopCredentialsConfiguration.PROXY_USERNAME_SUFFIX.getKey())
          .withPrefixes(KEY_PREFIXES);

  private static final HadoopConfigurationProperty<RedactedString> GCS_PROXY_PASSWORD =
      new HadoopConfigurationProperty<RedactedString>(
              HadoopCredentialsConfiguration.PROXY_PASSWORD_SUFFIX.getKey())
          .withPrefixes(KEY_PREFIXES);

  private static final HadoopConfigurationProperty<Long> GCS_HTTP_READ_TIMEOUT =
      new HadoopConfigurationProperty<>(
              HadoopCredentialsConfiguration.READ_TIMEOUT_SUFFIX.getKey(),
              HadoopCredentialsConfiguration.READ_TIMEOUT_SUFFIX.getDefault())
          .withPrefixes(KEY_PREFIXES);

  private static final ImmutableMap<String, String> HADOOP_TO_ANALYTICS_CORE_KEY_MAPPINGS =
      ImmutableMap.<String, String>builder()
          .put(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey(), PROJECT_ID_KEY)
          .put(GoogleHadoopFileSystemConfiguration.GCS_ROOT_URL.getKey(), SERVICE_HOST_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_VECTORED_READ_THREADS.getKey(),
              READ_THREAD_COUNT_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_VECTORED_READ_RANGE_MIN_SEEK.getKey(),
              MAX_MERGE_GAP_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_VECTORED_READ_MERGED_RANGE_MAX_SIZE.getKey(),
              MAX_MERGE_SIZE_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_INPLACE_SEEK_LIMIT.getKey(),
              INPLACE_SEEK_LIMIT_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_MIN_RANGE_REQUEST_SIZE.getKey(),
              RANDOM_READ_MIN_REQ_SIZE_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_FADVISE_REQUEST_TRACK_COUNT.getKey(),
              ADAPTIVE_READ_SEQ_THRESHOLD_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_FADVISE.getKey(),
              FILE_ACCESS_PATTERN_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_OUTPUT_STREAM_UPLOAD_CHUNK_SIZE.getKey(),
              UPLOAD_CHUNK_SIZE_KEY)
          .put(GoogleHadoopFileSystemConfiguration.GCS_CLIENT_UPLOAD_TYPE.getKey(), UPLOAD_TYPE_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_PCU_BUFFER_COUNT.getKey(),
              PCU_BUFFER_COUNT_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_PCU_BUFFER_CAPACITY.getKey(),
              PCU_BUFFER_CAPACITY_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_PCU_PART_FILE_CLEANUP_TYPE.getKey(),
              PCU_PART_FILE_CLEANUP_TYPE_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_PCU_PART_FILE_NAME_PREFIX.getKey(),
              PCU_PART_FILE_NAME_PREFIX_KEY)
          .put(
              GoogleHadoopFileSystemConfiguration.GCS_WRITE_ROLLING_CHECKSUM_ENABLE.getKey(),
              CHECKSUM_VALIDATION_ENABLED_KEY)
          .put(HADOOP_UNIVERSE_DOMAIN_KEY, UNIVERSE_DOMAIN_KEY)
          .build();

  private AnalyticsCoreConfigMapper() {
    // Utility class
  }

  /**
   * Maps configurations from Hadoop Configuration to a map suitable for Analytics Core, including
   * config-based authentication settings whenever supported by Analytics Core.
   *
   * @param config The Hadoop configuration.
   * @param prefix The prefix used for Analytics Core properties (e.g., "fs.gs.").
   * @return A map containing the mapped properties.
   */
  static Map<String, String> mapConfigs(Configuration config, String prefix) {
    return mapConfigs(config, prefix, /* includeAuthIdentity= */ true);
  }

  /**
   * Maps configurations from Hadoop Configuration to a map suitable for Analytics Core.
   *
   * @param config The Hadoop configuration.
   * @param prefix The prefix used for Analytics Core properties (e.g., "fs.gs.").
   * @param includeAuthIdentity Whether to map identity settings onto {@code
   *     analytics-core.auth.*}. Set to {@code false} when Hadoop injects a token-provider
   *     credential directly.
   * @return A map containing the mapped properties.
   */
  static Map<String, String> mapConfigs(
      Configuration config, String prefix, boolean includeAuthIdentity) {
    Map<String, String> mappedProperties = config.getValByRegex("^" + prefix.replace(".", "\\."));

    // Direct mappings from Connector to Analytics Core
    HADOOP_TO_ANALYTICS_CORE_KEY_MAPPINGS.forEach(
        (hadoopKey, analyticsKey) ->
            mapAndRemoveSource(hadoopKey, mappedProperties, prefix + analyticsKey));

    // Handle requester pays project ID: when enabled, use fs.gs.requester.pays.project.id if set,
    // otherwise fallback to fs.gs.project.id
    String requesterPaysMode =
        config.get(
            GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_MODE.getKey(),
            GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_MODE.getDefault().name());
    String requesterPaysProjectId =
        mappedProperties.remove(
            GoogleHadoopFileSystemConfiguration.GCS_REQUESTER_PAYS_PROJECT_ID.getKey());
    if (!RequesterPaysMode.DISABLED.name().equalsIgnoreCase(requesterPaysMode)) {
      if (requesterPaysProjectId == null || requesterPaysProjectId.isEmpty()) {
        requesterPaysProjectId =
            config.get(GoogleHadoopFileSystemConfiguration.GCS_PROJECT_ID.getKey());
      }
      if (requesterPaysProjectId != null && !requesterPaysProjectId.isEmpty()) {
        mappedProperties.put(prefix + USER_PROJECT_KEY, requesterPaysProjectId);
      }
    }

    // Handle temporary paths: use fs.gs.write.temporary.dirs if set, otherwise fallback to
    // hadoop.tmp.dir
    String tempPaths =
        mappedProperties.remove(
            GoogleHadoopFileSystemConfiguration.GCS_WRITE_TEMPORARY_FILES_PATH.getKey());
    if (tempPaths == null || tempPaths.isEmpty()) {
      tempPaths = config.get("hadoop.tmp.dir");
    }
    if (tempPaths != null && !tempPaths.isEmpty()) {
      mappedProperties.put(prefix + TEMPORARY_PATHS_KEY, tempPaths);
    }

    // User agent is computed from GHFS_ID and an optional suffix, not a simple 1:1 mapping.
    mappedProperties.put(
        prefix + USER_AGENT_KEY, GoogleHadoopFileSystemConfiguration.getApplicationName(config));

    // Ensure client.type is explicitly removed from mapped properties to prevent crashes
    mappedProperties.remove(GoogleHadoopFileSystemConfiguration.GCS_CLIENT_TYPE.getKey());

    mapSecret(
        config,
        mappedProperties,
        GoogleHadoopFileSystemConfiguration.GCS_ENCRYPTION_KEY,
        prefix + ENCRYPTION_KEY_KEY);
    mapSecret(
        config,
        mappedProperties,
        GoogleHadoopFileSystemConfiguration.GCS_ENCRYPTION_KEY,
        prefix + DECRYPTION_KEY_KEY);

    mapTransportConfigs(config, prefix, mappedProperties);
    if (includeAuthIdentity) {
      mapAuthIdentityConfigs(config, prefix, mappedProperties);
    }
    removeConnectorAuthKeys(prefix, mappedProperties);

    return mappedProperties;
  }

  private static void mapTransportConfigs(
      Configuration config, String prefix, Map<String, String> map) {
    mapStringProperty(
        config,
        map,
        GCS_PROXY_ADDRESS,
        GCS_CONFIG_PREFIX + HadoopCredentialsConfiguration.PROXY_ADDRESS_SUFFIX.getKey(),
        prefix + PROXY_ADDRESS_KEY);
    mapSecretProperty(
        config,
        map,
        GCS_PROXY_USERNAME,
        GCS_CONFIG_PREFIX + HadoopCredentialsConfiguration.PROXY_USERNAME_SUFFIX.getKey(),
        prefix + PROXY_USERNAME_KEY);
    mapSecretProperty(
        config,
        map,
        GCS_PROXY_PASSWORD,
        GCS_CONFIG_PREFIX + HadoopCredentialsConfiguration.PROXY_PASSWORD_SUFFIX.getKey(),
        prefix + PROXY_PASSWORD_KEY);

    mapTimeout(
        config,
        map,
        GoogleHadoopFileSystemConfiguration.GCS_HTTP_CONNECT_TIMEOUT,
        ImmutableList.of(GoogleHadoopFileSystemConfiguration.GCS_HTTP_CONNECT_TIMEOUT.getKey()),
        prefix + CONNECT_TIMEOUT_KEY);
    mapTimeout(
        config,
        map,
        GCS_HTTP_READ_TIMEOUT,
        ImmutableList.of(
            GCS_CONFIG_PREFIX + HadoopCredentialsConfiguration.READ_TIMEOUT_SUFFIX.getKey(),
            HadoopCredentialsConfiguration.BASE_KEY_PREFIX
                + HadoopCredentialsConfiguration.READ_TIMEOUT_SUFFIX.getKey()),
        prefix + READ_TIMEOUT_KEY);
  }

  private static void mapAuthIdentityConfigs(
      Configuration config, String prefix, Map<String, String> map) {
    AuthenticationType authType = AUTHENTICATION_TYPE.get(config, config::getEnum);
    if (authType == AuthenticationType.ACCESS_TOKEN_PROVIDER) {
      return;
    }
    map.putIfAbsent(prefix + AUTH_TYPE_KEY, toAnalyticsCoreAuthType(authType));

    switch (authType) {
      case SERVICE_ACCOUNT_JSON_KEYFILE:
        mapStringProperty(
            config,
            map,
            SERVICE_ACCOUNT_JSON_KEYFILE,
            GCS_CONFIG_PREFIX
                + HadoopCredentialsConfiguration.SERVICE_ACCOUNT_JSON_KEYFILE_SUFFIX.getKey(),
            prefix + SERVICE_ACCOUNT_JSON_KEYFILE_KEY);
        break;
      case WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE:
        mapStringProperty(
            config,
            map,
            WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE,
            GCS_CONFIG_PREFIX
                + HadoopCredentialsConfiguration
                    .WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE_SUFFIX
                    .getKey(),
            prefix + WORKLOAD_IDENTITY_CREDENTIAL_CONFIG_FILE_KEY);
        break;
      case USER_CREDENTIALS:
        mapStringProperty(
            config,
            map,
            AUTH_CLIENT_ID,
            GCS_CONFIG_PREFIX + HadoopCredentialsConfiguration.AUTH_CLIENT_ID_SUFFIX.getKey(),
            prefix + CLIENT_ID_KEY);
        mapSecretProperty(
            config,
            map,
            AUTH_CLIENT_SECRET,
            GCS_CONFIG_PREFIX + HadoopCredentialsConfiguration.AUTH_CLIENT_SECRET_SUFFIX.getKey(),
            prefix + CLIENT_SECRET_KEY);
        mapSecretProperty(
            config,
            map,
            AUTH_REFRESH_TOKEN,
            GCS_CONFIG_PREFIX + HadoopCredentialsConfiguration.AUTH_REFRESH_TOKEN_SUFFIX.getKey(),
            prefix + REFRESH_TOKEN_KEY);
        break;
      default:
        break;
    }

    mapImpersonationServiceAccount(config, prefix, map);

    mapStringProperty(
        config,
        map,
        TOKEN_SERVER_URL,
        GCS_CONFIG_PREFIX + HadoopCredentialsConfiguration.TOKEN_SERVER_URL_SUFFIX.getKey(),
        prefix + TOKEN_SERVER_URI_KEY);
  }

  private static void mapImpersonationServiceAccount(
      Configuration config, String prefix, Map<String, String> map) {
    try {
      Optional<String> impersonationServiceAccount =
          HadoopCredentialsConfiguration.getImpersonationServiceAccount(config, GCS_CONFIG_PREFIX);
      impersonationServiceAccount.ifPresent(
          account -> map.putIfAbsent(prefix + IMPERSONATION_SERVICE_ACCOUNT_KEY, account));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to resolve impersonation service account", e);
    }
  }

  /**
   * Removes connector-spelled auth and transport keys so that only the mapped {@code
   * analytics-core.*} keys reach Analytics Core.
   */
  private static void removeConnectorAuthKeys(String prefix, Map<String, String> map) {
    String authPrefix = prefix + "auth.";
    String proxyPrefix = prefix + "proxy.";
    map.keySet()
        .removeIf(
            key ->
                key.startsWith(authPrefix)
                    || key.startsWith(proxyPrefix)
                    || key.equals(
                        GCS_CONFIG_PREFIX
                            + HadoopCredentialsConfiguration.TOKEN_SERVER_URL_SUFFIX.getKey())
                    || key.equals(
                        GCS_CONFIG_PREFIX
                            + HadoopCredentialsConfiguration.READ_TIMEOUT_SUFFIX.getKey())
                    || key.equals(
                        GoogleHadoopFileSystemConfiguration.GCS_HTTP_CONNECT_TIMEOUT.getKey()));
  }

  private static String toAnalyticsCoreAuthType(AuthenticationType authType) {
    if (authType == AuthenticationType.WORKLOAD_IDENTITY_FEDERATION_CREDENTIAL_CONFIG_FILE) {
      return WORKLOAD_IDENTITY_FEDERATION_VALUE;
    }
    return authType.name();
  }

  private static void mapStringProperty(
      Configuration config,
      Map<String, String> map,
      HadoopConfigurationProperty<String> property,
      String sweptConnectorKey,
      String analyticsCoreKey) {
    map.remove(sweptConnectorKey);
    String value = property.get(config, config::get);
    if (value != null && !value.isEmpty()) {
      map.putIfAbsent(analyticsCoreKey, value);
    }
  }

  private static void mapSecretProperty(
      Configuration config,
      Map<String, String> map,
      HadoopConfigurationProperty<RedactedString> property,
      String sweptConnectorKey,
      String analyticsCoreKey) {
    map.remove(sweptConnectorKey);
    RedactedString secret = property.getPassword(config);
    if (secret != null) {
      map.putIfAbsent(analyticsCoreKey, secret.value());
    }
  }

  /**
   * Maps a secret, so that values held in a Hadoop {@code CredentialProvider} (jceks) are resolved.
   */
  private static void mapSecret(
      Configuration config,
      Map<String, String> map,
      HadoopConfigurationProperty<RedactedString> property,
      String analyticsCoreKey) {
    mapSecretProperty(config, map, property, property.getKey(), analyticsCoreKey);
  }

  /**
   * Maps a timeout, normalising it to the milliseconds Analytics Core expects.
   *
   * <p>Hadoop accepts suffixed durations such as {@code 30s}, which Analytics Core parses with
   * {@code Long.parseLong} and rejects. Only an explicitly set value is forwarded, so that an unset
   * timeout leaves the Analytics Core default in place.
   */
  private static void mapTimeout(
      Configuration config,
      Map<String, String> map,
      HadoopConfigurationProperty<Long> property,
      ImmutableList<String> candidateKeys,
      String analyticsCoreKey) {
    candidateKeys.forEach(map::remove);
    boolean isExplicitlySet = candidateKeys.stream().anyMatch(key -> config.get(key) != null);
    if (!isExplicitlySet) {
      return;
    }
    map.putIfAbsent(analyticsCoreKey, String.valueOf(property.getTimeDuration(config).toMillis()));
  }

  private static void mapAndRemoveSource(
      String hadoopKey, Map<String, String> map, String analyticsCoreKey) {
    String value = map.remove(hadoopKey);
    if (value != null) {
      if (hadoopKey.equals(GoogleHadoopFileSystemConfiguration.GCS_INPUT_STREAM_FADVISE.getKey())) {
        value = toFileAccessPattern(value);
      } else if (hadoopKey.equals(
          GoogleHadoopFileSystemConfiguration.GCS_CLIENT_UPLOAD_TYPE.getKey())) {
        value = toUploadType(value);
      } else if (hadoopKey.equals(
          GoogleHadoopFileSystemConfiguration.GCS_PCU_PART_FILE_CLEANUP_TYPE.getKey())) {
        value = toPartFileCleanupType(value);
      }
      map.putIfAbsent(analyticsCoreKey, value);
    }
  }

  private static String toUploadType(String uploadType) {
    String normalized = uploadType.replace('-', '_').toUpperCase();
    switch (normalized) {
      case "CHUNK_UPLOAD":
      case "WRITE_TO_DISK_THEN_UPLOAD":
      case "JOURNALING":
      case "PARALLEL_COMPOSITE_UPLOAD":
        return normalized;
      default:
        return GoogleHadoopFileSystemConfiguration.GCS_CLIENT_UPLOAD_TYPE.getDefault().name();
    }
  }

  private static String toPartFileCleanupType(String cleanupType) {
    String normalized = cleanupType.replace('-', '_').toUpperCase();
    switch (normalized) {
      case "ALWAYS":
      case "NEVER":
      case "ON_SUCCESS":
        return normalized;
      default:
        return GoogleHadoopFileSystemConfiguration.GCS_PCU_PART_FILE_CLEANUP_TYPE
            .getDefault()
            .name();
    }
  }

  private static String toFileAccessPattern(String fadvise) {
    switch (fadvise.toUpperCase()) {
      case "AUTO":
        return "AUTO_SEQUENTIAL";
      case "AUTO_RANDOM":
        return "AUTO_RANDOM";
      case "SEQUENTIAL":
        return "SEQUENTIAL";
      case "RANDOM":
        return "RANDOM";
      default:
        return "AUTO_SEQUENTIAL";
    }
  }
}
