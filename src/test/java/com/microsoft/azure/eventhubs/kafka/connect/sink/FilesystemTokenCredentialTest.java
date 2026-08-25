package com.microsoft.azure.eventhubs.kafka.connect.sink;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Base64;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class FilesystemTokenCredentialTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final TokenRequestContext REQUEST =
            new TokenRequestContext().addScopes("https://eventhubs.azure.net/.default");

    /** A syntactically valid JWT whose payload carries the given expiry. Signature is irrelevant here. */
    private static String jwt(Instant expiresAt, String marker) {
        Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
        String header = encoder.encodeToString("{\"alg\":\"RS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
        String payload = encoder.encodeToString(
                ("{\"aud\":\"" + marker + "\",\"exp\":" + expiresAt.getEpochSecond() + "}").getBytes(StandardCharsets.UTF_8));
        return header + "." + payload + ".signature";
    }

    private Path writeToken(String contents) throws IOException {
        Path file = folder.newFile("token").toPath();
        Files.write(file, contents.getBytes(StandardCharsets.UTF_8));
        return file;
    }

    @Test
    public void readsTheTokenAndItsExpiry() throws Exception {
        Instant expiry = Instant.now().plus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.SECONDS);
        String token = jwt(expiry, "first");
        AccessToken accessToken = new FilesystemTokenCredential(writeToken(token)).getToken(REQUEST).block();

        assertEquals(token, accessToken.getToken());
        assertEquals(OffsetDateTime.ofInstant(expiry, ZoneOffset.UTC), accessToken.getExpiresAt());
    }

    @Test
    public void trailingNewlinesAndBlankLinesAreIgnored() throws Exception {
        String token = jwt(Instant.now().plus(1, ChronoUnit.HOURS), "first");
        AccessToken accessToken =
                new FilesystemTokenCredential(writeToken("\n  " + token + "  \n\n")).getToken(REQUEST).block();
        assertEquals(token, accessToken.getToken());
    }

    @Test
    public void aValidTokenIsCachedRatherThanRereadPerAuthentication() throws Exception {
        Path file = writeToken(jwt(Instant.now().plus(1, ChronoUnit.HOURS), "first"));
        FilesystemTokenCredential credential = new FilesystemTokenCredential(file);

        String first = credential.getToken(REQUEST).block().getToken();
        Files.write(file, jwt(Instant.now().plus(1, ChronoUnit.HOURS), "second").getBytes(StandardCharsets.UTF_8));

        assertEquals(first, credential.getToken(REQUEST).block().getToken());
    }

    @Test
    public void aTokenNearingExpiryIsReloadedFromDisk() throws Exception {
        // Inside the refresh margin, so the sidecar's replacement must be picked up without a restart.
        Path file = writeToken(jwt(Instant.now().plus(1, ChronoUnit.MINUTES), "first"));
        FilesystemTokenCredential credential = new FilesystemTokenCredential(file);

        String first = credential.getToken(REQUEST).block().getToken();
        String rotated = jwt(Instant.now().plus(2, ChronoUnit.HOURS), "second");
        Files.write(file, rotated.getBytes(StandardCharsets.UTF_8));

        assertNotEquals(first, credential.getToken(REQUEST).block().getToken());
        assertEquals(rotated, credential.getToken(REQUEST).block().getToken());
    }

    @Test
    public void aTokenWithoutAnExpiryClaimGetsAShortAssumedLifetime() throws Exception {
        Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
        String noExp = encoder.encodeToString("{}".getBytes(StandardCharsets.UTF_8));
        AccessToken accessToken = new FilesystemTokenCredential(writeToken("h." + noExp + ".s"))
                .getToken(REQUEST).block();

        assertTrue(accessToken.getExpiresAt().isBefore(OffsetDateTime.now(ZoneOffset.UTC).plusMinutes(6)));
    }

    @Test
    public void anUnreadableTokenFileSurfacesAsARetriableIoFailure() {
        // A sidecar replacing the file non-atomically is transient, so EventHubErrors must see an IOException.
        Path missing = folder.getRoot().toPath().resolve("not-written-yet");
        UncheckedIOException ex = assertThrows(UncheckedIOException.class,
                () -> new FilesystemTokenCredential(missing).getToken(REQUEST).block());
        assertTrue(EventHubErrors.isRetriable(ex));
    }

    @Test
    public void anEmptyTokenFileIsReportedRatherThanReturnedAsAnEmptyToken() throws Exception {
        Path file = writeToken("\n\n");
        assertThrows(UncheckedIOException.class,
                () -> new FilesystemTokenCredential(file).getToken(REQUEST).block());
    }

    @Test
    public void tokenPathFollowsTheNamespaceAndEntityPathLayout() {
        EventHubConnectionString connectionString = EventHubConnectionString.parse(
                "Endpoint=sb://ns.servicebus.windows.net/;EntityPath=hub");
        // Same on-disk layout the v3 provider produced, so existing sidecars keep working.
        assertEquals(java.nio.file.Paths.get("/tokens/ns.servicebus.windows.net/hub"),
                FilesystemTokenCredential.forEventHub("/tokens", connectionString).tokenFile());
    }

    @Test
    public void missingTokenDirectoryIsAConfigurationError() {
        EventHubConnectionString connectionString = EventHubConnectionString.parse(
                "Endpoint=sb://ns.servicebus.windows.net/;EntityPath=hub");
        ConnectException ex = assertThrows(ConnectException.class,
                () -> FilesystemTokenCredential.forEventHub("  ", connectionString));
        assertTrue(ex.getMessage(), ex.getMessage().contains(EventHubSinkConfig.AUTH_TOKEN_DIR));
    }

    @Test
    public void jwtAuthWithoutAnEntityPathIsAConfigurationError() {
        EventHubConnectionString connectionString =
                EventHubConnectionString.parse("Endpoint=sb://ns.servicebus.windows.net/");
        assertThrows(ConnectException.class,
                () -> FilesystemTokenCredential.forEventHub("/tokens", connectionString));
    }
}
