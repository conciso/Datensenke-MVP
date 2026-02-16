package de.conciso.datensenke;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class FailureLogWriter {

    private static final Logger log = LoggerFactory.getLogger(FailureLogWriter.class);
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private final Path logPath;

    public FailureLogWriter(
            @Value("${datensenke.failure-log-path:datensenke-failures.log}") String failureLogPath) {
        this.logPath = Path.of(failureLogPath);
    }

    public void logFailure(String fileName, String reason, String trackId, String hash) {
        String timestamp = OffsetDateTime.now().format(TIMESTAMP_FORMAT);
        String line = String.format("%s | file=%s | reason=%s | track_id=%s | hash=%s%n",
                timestamp,
                fileName != null ? fileName : "",
                reason != null ? reason : "",
                trackId != null ? trackId : "",
                hash != null ? hash : "");
        try {
            Files.writeString(logPath, line,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.error("Failed to write failure log entry for {}: {}", fileName, e.getMessage());
        }
    }
}
