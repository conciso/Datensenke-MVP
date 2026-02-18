package de.conciso.datensenke.preprocessor;

import java.nio.file.Path;

/**
 * Pre-processes a downloaded file before it is uploaded to LightRAG.
 *
 * Implementations receive the path of the downloaded file and the original
 * file name, and return the path of the file that should be uploaded.
 * The returned path may be the same as the input (in-place / no-op) or a
 * new temporary file created by the preprocessor.
 *
 * The caller is responsible for cleaning up both the original downloaded file
 * and any new file returned by the preprocessor.
 */
public interface FilePreprocessor {

    /**
     * @param file             path to the downloaded file
     * @param originalFileName the original file name (for logging / naming)
     * @return path of the file to upload — either {@code file} itself or a new temp file
     */
    Path process(Path file, String originalFileName) throws Exception;
}
