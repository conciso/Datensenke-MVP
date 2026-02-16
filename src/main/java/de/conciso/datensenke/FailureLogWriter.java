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
            @Value("${datensenke.failure-log-path:logs/datensenke-failures.log}") String failureLogPath) {
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
            Path parent = logPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(logPath, line,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.error("Failed to write failure log entry for {}: {}", fileName, e.getMessage());
        }
    }

    public boolean isAlreadyLogged(String trackId) {
        if (trackId == null || !Files.exists(logPath)) {
            return false;
        }
        try {
            String content = Files.readString(logPath);
            return content.contains("track_id=" + trackId);
        } catch (IOException e) {
            log.warn("Failed to read failure log for dedup check: {}", e.getMessage());
            return false;
        }
    }
}
