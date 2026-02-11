package de.conciso.datensenke;

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
            @Value("${datensenke.remote.directory}") String directory) {

        log.info("Configuring remote file source: protocol={}, host={}, port={}, directory={}",
                protocol, host, port, directory);

        return switch (protocol.toLowerCase()) {
            case "sftp" -> new SftpFileSource(host, port, username, password, directory);
            case "ftp" -> new FtpFileSource(host, port, username, password, directory);
            default -> throw new IllegalArgumentException(
                    "Unsupported protocol: " + protocol + ". Use 'sftp' or 'ftp'.");
        };
    }
}
