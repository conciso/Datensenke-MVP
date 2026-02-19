package de.conciso.datensenke.remote;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SftpFileSource implements RemoteFileSource {

    private static final Logger log = LoggerFactory.getLogger(SftpFileSource.class);

    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final String privateKey;
    private final String directory;
    private final List<String> allowedExtensions;

    public SftpFileSource(String host, int port, String username, String password, String privateKey, String directory, List<String> allowedExtensions) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.privateKey = privateKey;
        this.directory = directory;
        this.allowedExtensions = allowedExtensions;
    }

    @Override
    public List<String> allowedExtensions() {
        return allowedExtensions;
    }

    @Override
    public List<RemoteFileInfo> listFiles() {
        List<RemoteFileInfo> result = new ArrayList<>();
        Session session = null;
        ChannelSftp channel = null;

        try {
            session = createSession();
            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();

            @SuppressWarnings("unchecked")
            Vector<ChannelSftp.LsEntry> entries = channel.ls(directory);

            for (ChannelSftp.LsEntry entry : entries) {
                String name = entry.getFilename();
                if (isSupportedFile(name) && !entry.getAttrs().isDir()) {
                    long mTime = entry.getAttrs().getMTime() * 1000L;
                    result.add(new RemoteFileInfo(name, mTime));
                }
            }

            log.debug("SFTP listed {} files in {}", result.size(), directory);
        } catch (Exception e) {
            log.error("SFTP listing failed: {}", e.getMessage());
        } finally {
            disconnect(channel, session);
        }

        return result;
    }

    @Override
    public Path downloadFile(String fileName) {
        Session session = null;
        ChannelSftp channel = null;

        try {
            Path tempFile = Files.createTempFile("datensenke-", "-" + fileName);
            session = createSession();
            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect();

            String remotePath = directory.endsWith("/")
                    ? directory + fileName
                    : directory + "/" + fileName;

            try (OutputStream out = Files.newOutputStream(tempFile)) {
                channel.get(remotePath, out);
            }

            log.debug("SFTP downloaded {} to {}", fileName, tempFile);
            return tempFile;
        } catch (SftpException | IOException e) {
            throw new RuntimeException("SFTP download failed for " + fileName, e);
        } catch (Exception e) {
            throw new RuntimeException("SFTP connection failed for " + fileName, e);
        } finally {
            disconnect(channel, session);
        }
    }

    private Session createSession() throws Exception {
        JSch jsch = new JSch();
        if (privateKey != null && !privateKey.isBlank()) {
            jsch.addIdentity(privateKey);
        }
        Session session = jsch.getSession(username, host, port);
        if (privateKey == null || privateKey.isBlank()) {
            session.setPassword(password);
        }
        session.setConfig("StrictHostKeyChecking", "no");
        session.setConfig("PreferredAuthentications", "publickey,password");
        session.setConfig("PubkeyAcceptedAlgorithms", "rsa-sha2-256,rsa-sha2-512,ssh-rsa,ecdsa-sha2-nistp256,ecdsa-sha2-nistp521,ssh-ed25519");
        session.connect();
        return session;
    }

    private void disconnect(ChannelSftp channel, Session session) {
        if (channel != null && channel.isConnected()) {
            channel.disconnect();
        }
        if (session != null && session.isConnected()) {
            session.disconnect();
        }
    }
}
