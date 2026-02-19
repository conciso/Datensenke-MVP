package de.conciso.datensenke.remote;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileSource implements RemoteFileSource {

    private static final Logger log = LoggerFactory.getLogger(LocalFileSource.class);

    private final Path directory;
    private final List<String> allowedExtensions;

    public LocalFileSource(String directory, List<String> allowedExtensions) {
        this.directory = Path.of(directory);
        this.allowedExtensions = allowedExtensions;
    }

    @Override
    public List<String> allowedExtensions() {
        return allowedExtensions;
    }

    @Override
    public List<RemoteFileInfo> listFiles() {
        List<RemoteFileInfo> result = new ArrayList<>();

        try (Stream<Path> files = Files.list(directory)) {
            files.filter(p -> !Files.isDirectory(p))
                    .filter(p -> isSupportedFile(p.getFileName().toString()))
                    .forEach(p -> {
                        try {
                            long mTime = Files.getLastModifiedTime(p).toMillis();
                            result.add(new RemoteFileInfo(p.getFileName().toString(), mTime));
                        } catch (IOException e) {
                            log.warn("Could not read last modified time for {}: {}", p, e.getMessage());
                        }
                    });

            log.debug("Local listed {} files in {}", result.size(), directory);
        } catch (IOException e) {
            log.error("Local listing failed for {}: {}", directory, e.getMessage());
        }

        return result;
    }

    @Override
    public Path downloadFile(String fileName) {
        try {
            Path source = directory.resolve(fileName);
            Path tempFile = Files.createTempFile("datensenke-", "-" + fileName);
            Files.copy(source, tempFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

            log.debug("Local copied {} to {}", fileName, tempFile);
            return tempFile;
        } catch (IOException e) {
            throw new RuntimeException("Local copy failed for " + fileName, e);
        }
    }
}
