package de.conciso.datensenke;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FtpFileSource implements RemoteFileSource {

    private static final Logger log = LoggerFactory.getLogger(FtpFileSource.class);

    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final String directory;

    public FtpFileSource(String host, int port, String username, String password, String directory) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.directory = directory;
    }

    @Override
    public List<RemoteFileInfo> listPdfFiles() {
        List<RemoteFileInfo> result = new ArrayList<>();
        FTPClient ftp = new FTPClient();

        try {
            connect(ftp);

            FTPFile[] files = ftp.listFiles(directory);
            for (FTPFile file : files) {
                String name = file.getName();
                if (file.isFile() && name.toLowerCase().endsWith(".pdf")) {
                    long mTime = file.getTimestamp().getTimeInMillis();
                    result.add(new RemoteFileInfo(name, mTime));
                }
            }

            log.debug("FTP listed {} PDF files in {}", result.size(), directory);
        } catch (IOException e) {
            log.error("FTP listing failed: {}", e.getMessage());
        } finally {
            disconnect(ftp);
        }

        return result;
    }

    @Override
    public Path downloadFile(String fileName) {
        FTPClient ftp = new FTPClient();

        try {
            Path tempFile = Files.createTempFile("datensenke-", "-" + fileName);
            connect(ftp);

            String remotePath = directory.endsWith("/")
                    ? directory + fileName
                    : directory + "/" + fileName;

            try (OutputStream out = Files.newOutputStream(tempFile)) {
                boolean success = ftp.retrieveFile(remotePath, out);
                if (!success) {
                    Files.deleteIfExists(tempFile);
                    throw new IOException("FTP retrieveFile returned false for " + remotePath);
                }
            }

            log.debug("FTP downloaded {} to {}", fileName, tempFile);
            return tempFile;
        } catch (IOException e) {
            throw new RuntimeException("FTP download failed for " + fileName, e);
        } finally {
            disconnect(ftp);
        }
    }

    private void connect(FTPClient ftp) throws IOException {
        ftp.connect(host, port);
        ftp.login(username, password);
        ftp.enterLocalPassiveMode();
        ftp.setFileType(FTP.BINARY_FILE_TYPE);
    }

    private void disconnect(FTPClient ftp) {
        try {
            if (ftp.isConnected()) {
                ftp.logout();
                ftp.disconnect();
            }
        } catch (IOException e) {
            log.debug("FTP disconnect error: {}", e.getMessage());
        }
    }
}
