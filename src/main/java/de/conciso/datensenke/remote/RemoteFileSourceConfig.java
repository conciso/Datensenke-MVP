package de.conciso.datensenke.remote;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RemoteFileSourceConfig {

    private static final Logger log = LoggerFactory.getLogger(RemoteFileSourceConfig.class);

    @Bean
    public RemoteFileSource remoteFileSource(
            @Value("${datensenke.remote.protocol}") String protocol,
            @Value("${datensenke.remote.host}") String host,
            @Value("${datensenke.remote.port}") int port,
            @Value("${datensenke.remote.username}") String username,
            @Value("${datensenke.remote.password}") String password,
            @Value("${datensenke.remote.private-key:}") String privateKey,
            @Value("${datensenke.remote.directory}") String directory,
            @Value("${datensenke.allowed-extensions:.pdf,.doc,.docx}") List<String> allowedExtensions) {

        log.info("Configuring file source: protocol={}, host={}, port={}, directory={}, allowedExtensions={}",
                protocol, host, port, directory, allowedExtensions);

        return switch (protocol.toLowerCase()) {
            case "sftp" -> new SftpFileSource(host, port, username, password, privateKey, directory, allowedExtensions);
            case "ftp" -> new FtpFileSource(host, port, username, password, directory, allowedExtensions);
            case "local" -> new LocalFileSource(directory, allowedExtensions);
            default -> throw new IllegalArgumentException(
                    "Unsupported protocol: " + protocol + ". Use 'sftp', 'ftp', or 'local'.");
        };
    }
}
