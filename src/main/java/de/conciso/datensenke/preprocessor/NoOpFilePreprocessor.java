package de.conciso.datensenke.preprocessor;

import java.nio.file.Path;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Default preprocessor — passes files through unchanged.
 * Active when {@code datensenke.preprocessor.enabled} is {@code false} or not set.
 */
@Component
@ConditionalOnProperty(name = "datensenke.preprocessor.enabled", havingValue = "false", matchIfMissing = true)
public class NoOpFilePreprocessor implements FilePreprocessor {

    @Override
    public Path process(Path file, String originalFileName) {
        return file;
    }
}
