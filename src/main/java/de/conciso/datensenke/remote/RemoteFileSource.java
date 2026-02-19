package de.conciso.datensenke.remote;

import java.nio.file.Path;
import java.util.List;

public interface RemoteFileSource {

    List<RemoteFileInfo> listFiles();

    Path downloadFile(String fileName);

    List<String> allowedExtensions();

    default boolean isSupportedFile(String name) {
        String lower = name.toLowerCase();
        return allowedExtensions().stream().anyMatch(lower::endsWith);
    }
}
