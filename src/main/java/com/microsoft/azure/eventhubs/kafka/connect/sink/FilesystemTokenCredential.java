package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Supplies Event Hubs credentials from a JWT written to the local filesystem by a sidecar.
 *
 * <p>Replaces the v3 {@code ITokenProvider}. Two things changed deliberately in the port:
 *
 * <ul>
 *   <li><b>The path is derived from the connection string, not from what the SDK passes in.</b> The v3 provider
 *       built the path out of the {@code resource} argument via {@code substring(indexOf("/"))}, which happened to
 *       produce the right answer for {@code amqps://host/entity} and nothing else. The v5 SDK passes AAD-style
 *       scopes, not a resource path, so that derivation would have silently broken. The layout on disk is
 *       unchanged: {@code <dir>/<namespace>/<entityPath>}.</li>
 *   <li><b>The token is cached until shortly before it expires.</b> The v3 provider re-read and re-parsed the file
 *       on every AMQP authentication, on the common ForkJoinPool.</li>
 * </ul>
 *
 * <p>The file is re-read whenever the cached token is within {@link #REFRESH_MARGIN} of expiry, so a sidecar that
 * rotates the file in place is picked up without restarting the connector.
 */
final class FilesystemTokenCredential implements TokenCredential {

    /** Re-read the token file this long before the current token expires. */
    private static final Duration REFRESH_MARGIN = Duration.ofMinutes(5);

    /**
     * Assumed lifetime for a token whose {@code exp} claim cannot be read. Short on purpose: a wrong-but-long
     * guess would keep a dead token in the cache and turn a rotation into an outage.
     */
    private static final Duration FALLBACK_LIFETIME = Duration.ofMinutes(5);

    private static final Pattern EXP_CLAIM = Pattern.compile("\"exp\"\\s*:\\s*(\\d+)");

    private static final Logger log = LoggerFactory.getLogger(FilesystemTokenCredential.class);

    private final Path tokenFile;

    private volatile AccessToken cached;

    FilesystemTokenCredential(Path tokenFile) {
        this.tokenFile = tokenFile;
    }

    /** Visible for testing. */
    Path tokenFile() {
        return tokenFile;
    }

    /**
     * Resolves the token file for an Event Hub, preserving the {@code <dir>/<namespace>/<entityPath>} layout that
     * the v3 provider produced.
     */
    static FilesystemTokenCredential forEventHub(String baseDirectory, EventHubConnectionString connectionString) {
        if (baseDirectory == null || baseDirectory.trim().isEmpty()) {
            throw new ConnectException("[EVENTHUB_CONFIG] " + EventHubSinkConfig.AUTHENTICATION_PROVIDER + "="
                    + EventHubSinkConfig.JWT_AUTHENTICATION_PROVIDER + " but no token directory is configured. Set "
                    + EventHubSinkConfig.AUTH_TOKEN_DIR + " or the " + EventHubSinkConfig.AUTH_TOKEN_DIR_ENV
                    + " environment variable.");
        }
        String entityPath = connectionString.entityPath();
        if (entityPath == null) {
            throw new ConnectException("[EVENTHUB_CONFIG] " + EventHubSinkConfig.AUTHENTICATION_PROVIDER + "="
                    + EventHubSinkConfig.JWT_AUTHENTICATION_PROVIDER + " but " + EventHubSinkConfig.CONNECTION_STRING
                    + " has no EntityPath, so the token file path cannot be resolved.");
        }
        Path file = Paths.get(baseDirectory, connectionString.fullyQualifiedNamespace(), entityPath);
        log.info("Filesystem JWT credential will read tokens from {}", file);
        return new FilesystemTokenCredential(file);
    }

    @Override
    public Mono<AccessToken> getToken(TokenRequestContext request) {
        // Mono.fromCallable keeps the blocking file read off the reactor event loop; the SDK subscribes on its own
        // scheduler. Returning an already-read value from a cache hit costs nothing.
        return Mono.fromCallable(this::getTokenSync);
    }

    @Override
    public AccessToken getTokenSync(TokenRequestContext request) {
        return getTokenSync();
    }

    private AccessToken getTokenSync() {
        AccessToken current = cached;
        if (current != null && OffsetDateTime.now(ZoneOffset.UTC).isBefore(current.getExpiresAt().minus(REFRESH_MARGIN))) {
            return current;
        }
        synchronized (this) {
            // Re-check: several AMQP links can hit an expiring token at once, and one file read is enough.
            current = cached;
            if (current != null && OffsetDateTime.now(ZoneOffset.UTC).isBefore(current.getExpiresAt().minus(REFRESH_MARGIN))) {
                return current;
            }
            AccessToken refreshed = read();
            cached = refreshed;
            return refreshed;
        }
    }

    private AccessToken read() {
        String jwt;
        try {
            List<String> lines = Files.readAllLines(tokenFile, StandardCharsets.UTF_8);
            jwt = lines.stream().map(String::trim).filter(line -> !line.isEmpty()).findFirst()
                    .orElseThrow(() -> new IOException("token file is empty"));
        } catch (IOException ex) {
            // Wrapped, not swallowed: EventHubErrors treats an IOException as retriable, which is right for a
            // token file that a sidecar is in the middle of replacing.
            throw new UncheckedIOException("Unable to read Event Hubs JWT from " + tokenFile, ex);
        }
        OffsetDateTime expiresAt = expiryOf(jwt);
        log.debug("Loaded Event Hubs JWT from {}, expires at {}", tokenFile, expiresAt);
        return new AccessToken(jwt, expiresAt);
    }

    /** Reads the {@code exp} claim out of a JWT payload without pulling in a JWT library. */
    private OffsetDateTime expiryOf(String jwt) {
        OffsetDateTime fallback = OffsetDateTime.now(ZoneOffset.UTC).plus(FALLBACK_LIFETIME);
        int firstDot = jwt.indexOf('.');
        int secondDot = firstDot < 0 ? -1 : jwt.indexOf('.', firstDot + 1);
        if (secondDot < 0) {
            log.warn("Token in {} is not a three-part JWT; assuming it expires in {}", tokenFile, FALLBACK_LIFETIME);
            return fallback;
        }
        try {
            String payload = new String(
                    Base64.getUrlDecoder().decode(jwt.substring(firstDot + 1, secondDot)), StandardCharsets.UTF_8);
            Matcher matcher = EXP_CLAIM.matcher(payload);
            if (matcher.find()) {
                return OffsetDateTime.ofInstant(Instant.ofEpochSecond(Long.parseLong(matcher.group(1))), ZoneOffset.UTC);
            }
            log.warn("Token in {} has no 'exp' claim; assuming it expires in {}", tokenFile, FALLBACK_LIFETIME);
        } catch (IllegalArgumentException ex) {
            log.warn("Unable to decode the JWT payload in {}; assuming it expires in {}", tokenFile, FALLBACK_LIFETIME);
        }
        return fallback;
    }
}
