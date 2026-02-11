package de.conciso.datensenke;

import java.nio.file.Path;
import java.util.List;

public interface RemoteFileSource {

    List<RemoteFileInfo> listPdfFiles();

    Path downloadFile(String fileName);
}
