package de.conciso.datensenke.preprocessor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Preprocessor that delegates to an external program (e.g. a Python script).
 * Active when {@code datensenke.preprocessor.enabled=true}.
 *
 * The command is called with two positional arguments:
 *   <command> <input_path> <output_path>
 *
 * The external program must write its result to {@code output_path} and exit
 * with code 0. Any output to stdout/stderr is logged on failure.
 *
 * Example config:
 *   datensenke.preprocessor.enabled=true
 *   datensenke.preprocessor.command=python3 /opt/datensenke/preprocess.py
 *   datensenke.preprocessor.timeout-seconds=120
 */
@Component
@ConditionalOnProperty(name = "datensenke.preprocessor.enabled", havingValue = "true")
public class ExternalFilePreprocessor implements FilePreprocessor {

    private static final Logger log = LoggerFactory.getLogger(ExternalFilePreprocessor.class);

    private final List<String> commandParts;
    private final int timeoutSeconds;

    public ExternalFilePreprocessor(
            @Value("${datensenke.preprocessor.command}") String command,
            @Value("${datensenke.preprocessor.timeout-seconds:120}") int timeoutSeconds) {
        this.commandParts = Arrays.asList(command.split("\\s+"));
        this.timeoutSeconds = timeoutSeconds;
        log.info("External preprocessor configured: {} (timeout={}s)", command, timeoutSeconds);
    }

    @Override
    public Path process(Path file, String originalFileName) throws Exception {
        Path outputFile = Files.createTempFile("datensenke-pre-", "-" + originalFileName);
        try {
            List<String> cmd = new ArrayList<>(commandParts);
            cmd.add(file.toString());
            cmd.add(outputFile.toString());

            log.debug("Preprocessing {}: {}", originalFileName, cmd);

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(true); // merge stderr into stdout for simpler capture
            Process process = pb.start();

            // Read output before waitFor to avoid blocking on full pipe buffers
            String output = new String(process.getInputStream().readAllBytes()).trim();

            boolean finished = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                throw new RuntimeException(
                        "Preprocessor timed out after " + timeoutSeconds + "s for: " + originalFileName);
            }

            if (process.exitValue() != 0) {
                throw new RuntimeException(
                        "Preprocessor exited with code " + process.exitValue()
                        + " for " + originalFileName
                        + (output.isEmpty() ? "" : ": " + output));
            }

            if (!output.isEmpty()) {
                log.debug("Preprocessor output for {}: {}", originalFileName, output);
            }
            log.info("Preprocessed: {}", originalFileName);
            return outputFile;

        } catch (Exception e) {
            Files.deleteIfExists(outputFile);
            throw e;
        }
    }
}
