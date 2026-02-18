package de.conciso.datensenke.remote;

import java.nio.file.Path;
import java.util.List;

public interface RemoteFileSource {

    List<RemoteFileInfo> listFiles();

    Path downloadFile(String fileName);

    default boolean isSupportedFile(String name) {
        String lower = name.toLowerCase();
        return lower.endsWith(".pdf") || lower.endsWith(".doc") || lower.endsWith(".docx");
    }
}
