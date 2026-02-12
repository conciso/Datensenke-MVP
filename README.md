# Datensenke MVP

Proof of Concept: Eine Spring-Boot-Anwendung, die ein Verzeichnis per SFTP, FTP oder lokal auf PDF-Dateien ueberwacht und Aenderungen (Create, Update, Delete) automatisch per REST-API an [LightRAG](https://github.com/HKUDS/LightRAG) weitergibt.

## Architektur

```
┌─────────────────┐   SFTP/FTP/Local  ┌──────────────┐
│  Datei-Quelle   │ ◄──────────────► │  Datensenke  │
│  (PDF-Dateien)  │                  │  (Spring Boot)│
└─────────────────┘                  └──────┬───────┘
                                            │ REST
                                            ▼
                                     ┌──────────────┐
                                     │   LightRAG   │
                                     │   API Server  │
                                     └──────────────┘
```

- **Polling** im konfigurierbaren Intervall (Default: 60s)
- **Protokoll** waehlbar: SFTP (SSH), FTP oder Local (lokaler Ordner)
- **Neue PDF** auf Remote-Server → Download + Upload an LightRAG
- **Geaenderte PDF** (lastModified) → Delete + Re-Upload
- **Geloeschte PDF** → Delete in LightRAG

## Voraussetzungen

- Docker & Docker Compose
- Externes Docker-Netzwerk `ki-playground` (wird von LightRAG mitgenutzt)
- Laufende LightRAG-Instanz im selben Netzwerk
- Erreichbarer SFTP- oder FTP-Server mit PDF-Dateien (oder ein lokaler Ordner)

## Schnellstart

### 1. Docker-Netzwerk erstellen (einmalig)

```bash
docker network create ki-playground
```

### 2. LightRAG starten

```bash
docker compose -f LightRAG/docker-compose-lightrag.yml up -d
```

LightRAG WebUI ist erreichbar unter: http://localhost:9622

### 3. Konfiguration anlegen

```bash
cp .env.example .env
# .env editieren: API Key, Remote-Server-Zugangsdaten anpassen
```

### 4. Datensenke starten

```bash
docker compose up --build -d
```

### 5. PDFs auf Remote-Server ablegen

Die Datensenke verbindet sich per SFTP/FTP zum konfigurierten Server (oder ueberwacht einen lokalen Ordner), erkennt neue PDF-Dateien beim naechsten Polling-Durchlauf und laedt sie an LightRAG hoch.

## Konfiguration

Alle Einstellungen werden ueber eine `.env`-Datei gesteuert (siehe `.env.example`):

| Variable | Default | Beschreibung |
|---|---|---|
| `DATENSENKE_LIGHTRAG_URL` | `http://lightrag:9621` | URL der LightRAG-API |
| `DATENSENKE_LIGHTRAG_API_KEY` | _(leer)_ | API Key fuer LightRAG-Authentifizierung |
| `DATENSENKE_POLL_INTERVAL_MS` | `60000` | Polling-Intervall in Millisekunden |
| `DATENSENKE_REMOTE_PROTOCOL` | `sftp` | Protokoll: `sftp`, `ftp` oder `local` |
| `DATENSENKE_REMOTE_HOST` | _(leer)_ | Hostname/IP des Remote-Servers |
| `DATENSENKE_REMOTE_PORT` | `22` | Port (22 fuer SFTP, 21 fuer FTP) |
| `DATENSENKE_REMOTE_USERNAME` | _(leer)_ | Benutzername |
| `DATENSENKE_REMOTE_PASSWORD` | _(leer)_ | Passwort |
| `DATENSENKE_REMOTE_DIRECTORY` | `/documents` | Verzeichnis auf dem Remote-Server (bei `local`: lokaler Pfad) |
| `DATENSENKE_STARTUP_SYNC` | `none` | Startup-Sync-Modus: `none`, `upload` oder `full` (siehe unten) |

## Startup-Sync

Beim Neustart ist der In-Memory-State leer. Ohne Sync wuerden alle Dateien beim ersten Poll erneut hochgeladen, und waehrend der Downtime geloeschte Dateien blieben als Waisen in LightRAG. Der Startup-Sync gleicht Quelle und LightRAG beim Start ab.

### Modi

| Modus | Verhalten |
|-------|-----------|
| `none` | Kein Sync. Alle Dateien werden beim ersten Poll als CREATE behandelt (rueckwaertskompatibel). |
| `upload` | Fehlende Dateien hochladen + veraenderte Dateien ersetzen. Keine Loeschungen in LightRAG. |
| `full` | Wie `upload` + Waisen und Duplikate aus LightRAG loeschen. |

### Content-Hash-Erkennung

Die Datensenke bettet beim Upload einen MD5-Hash des Dateiinhalts in den Dateinamen ein:

```
datensenke-{md5hash}-{originalName}.pdf
```

Beim naechsten Start wird der Hash der lokalen Datei mit dem im LightRAG-`file_path` gespeicherten Hash verglichen. Stimmen sie nicht ueberein, gilt das Dokument als veraltet (stale) und wird geloescht + neu hochgeladen. Das deckt das Szenario ab, in dem eine Datei waehrend der Downtime ueberschrieben wird.

**Beispiel:**

1. `Max Mustermann.pdf` wird hochgeladen → LightRAG speichert `datensenke-a1b2c3...-Max Mustermann.pdf`
2. Datensenke wird gestoppt
3. Jemand ueberschreibt `Max Mustermann.pdf` mit neuem Inhalt
4. Datensenke startet → lokaler Hash `d4e5f6...` ≠ gespeicherter Hash `a1b2c3...` → STALE → Delete + Re-Upload

**Hinweis:** Dokumente, die vor Einfuehrung des Hash-Features hochgeladen wurden (Legacy-Uploads ohne eingebetteten Hash), werden beim naechsten Startup-Sync automatisch als stale erkannt und neu hochgeladen.

### Persistierter State (`.datensenke-state.json`)

Der Datei-State (Dateiname, MD5-Hash, lastModified) wird in `.datensenke-state.json` im Arbeitsverzeichnis persistiert. Damit muss beim Startup nicht jede Quelldatei erneut heruntergeladen werden, um den Hash zu berechnen:

- **`lastModified` unveraendert** → persistierter Hash wird wiederverwendet (kein Download)
- **`lastModified` geaendert oder kein State vorhanden** → Datei wird heruntergeladen und gehasht

Im Normalfall (keine Aenderungen waehrend Downtime) werden beim Startup **keine Dateien heruntergeladen**.

## Projektstruktur

```
Datensenke-MVP/
├── pom.xml                          # Spring Boot 3.4, Java 21
├── Dockerfile                       # Multi-Stage Build
├── docker-compose.yml               # Datensenke Service
├── LightRAG/
│   └── docker-compose-lightrag.yml  # LightRAG Service
└── src/main/
    ├── java/de/conciso/datensenke/
    │   ├── DatensenkeApplication.java     # Main + @EnableScheduling
    │   ├── FileWatcherService.java        # Polling-Logik + Startup-Sync
    │   ├── LightRagClient.java            # REST-Client fuer LightRAG
    │   ├── LightRagBusyException.java     # Exception bei LightRAG-Processing
    │   ├── RemoteFileSource.java          # Interface fuer Remote-Zugriff
    │   ├── RemoteFileInfo.java            # Record fuer Datei-Metadaten
    │   ├── RemoteFileSourceConfig.java    # Bean-Konfiguration (SFTP/FTP/Local)
    │   ├── SftpFileSource.java            # SFTP-Implementierung (JSch)
    │   ├── FtpFileSource.java             # FTP-Implementierung (Commons Net)
    │   └── LocalFileSource.java           # Local-Implementierung (java.nio.file)
    └── resources/
        └── application.yml                # Default-Konfiguration
```

## Logs pruefen

```bash
docker compose logs -f datensenke
```

Erwartete Log-Ausgaben:

```
CREATE: dokument.pdf
UPDATE: dokument.pdf (delete + re-upload)
DELETE: dokument.pdf
```

## Lokale Entwicklung

```bash
./mvnw spring-boot:run
```

Dabei die Properties anpassen (`application.yml`) oder per Environment-Variablen uebersteuern, z.B.:

```bash
DATENSENKE_REMOTE_HOST=myserver.local DATENSENKE_REMOTE_USERNAME=user DATENSENKE_REMOTE_PASSWORD=pass ./mvnw spring-boot:run
```

## Randfaelle und deren Behandlung

| Szenario | Problem | Loesung |
|----------|---------|---------|
| **Delete waehrend LightRAG-Processing** | Datei wird aus dem Ordner entfernt, waehrend LightRAG sie noch verarbeitet. LightRAG antwortet mit `status: "busy"` und ignoriert den Delete. Nach dem Processing bleibt das Dokument als Waise in LightRAG. | Die Datensenke merkt sich die Doc-ID und versucht den Delete bei jedem Poll-Zyklus erneut, bis LightRAG ihn akzeptiert. |
| **Update waehrend LightRAG-Processing** | Datei wird im Ordner ueberschrieben, waehrend LightRAG die alte Version noch verarbeitet. Der fuer das Update noetige Delete schlaegt fehl (`status: "busy"`). | Der alte `lastModified`-Wert bleibt im State erhalten. Beim naechsten Poll wird die Aenderung erneut erkannt und der Update-Zyklus (Delete + Re-Upload) wiederholt, bis LightRAG bereit ist. |
| **Datei waehrend Downtime ueberschrieben** | Datensenke ist gestoppt, jemand ersetzt eine PDF mit neuem Inhalt. Beim Neustart existiert der Dateiname bereits in LightRAG — die Aenderung wird nicht erkannt. | Beim Upload wird ein MD5-Hash in den Dateinamen eingebettet (`datensenke-{hash}-{name}.pdf`). Beim Startup vergleicht der Sync den lokalen Hash mit dem in LightRAG gespeicherten. Bei Abweichung → Delete + Re-Upload. |
| **Datei waehrend Downtime geloescht** | Datensenke ist gestoppt, eine PDF wird aus dem Ordner entfernt. Beim Neustart ist der In-Memory-State leer, die Waise in LightRAG wird nicht erkannt. | Startup-Sync im Modus `full` erkennt Dokumente in LightRAG, die keiner Quelldatei mehr zugeordnet werden koennen, und loescht sie. |
| **Datei waehrend Downtime hinzugefuegt** | Neue PDF wird in den Ordner gelegt, waehrend die Datensenke gestoppt ist. | Startup-Sync (Modi `upload` und `full`) erkennt Dateien, die in der Quelle aber nicht in LightRAG vorhanden sind, und laedt sie hoch. |
| **Duplikate in LightRAG** | Durch Neustarts oder Race Conditions entstehen mehrere LightRAG-Dokumente fuer dieselbe Quelldatei. | Startup-Sync im Modus `full` erkennt Duplikate, behaelt das neueste (nach `created_at`) und loescht den Rest. |
| **Legacy-Uploads ohne Hash** | Dokumente, die vor Einfuehrung der Hash-Erkennung hochgeladen wurden, haben keinen eingebetteten MD5-Hash im Dateinamen. | Beim Startup-Sync wird ein fehlender Hash als Abweichung gewertet → automatischer Re-Upload mit eingebettetem Hash. |
| **State-Datei fehlt oder beschaedigt** | `.datensenke-state.json` wurde geloescht oder ist nicht lesbar. | Alle Quelldateien werden heruntergeladen und gehasht (einmaliger Mehraufwand). Danach wird die State-Datei neu geschrieben. |
| **Erster Start (kein State, kein LightRAG-Inhalt)** | Weder State-Datei noch Dokumente in LightRAG vorhanden. | Startup-Sync laedt alle Quelldateien hoch. Bei `none` werden sie beim ersten Poll als CREATE behandelt. |

## Technologie-Stack

- Java 21
- Spring Boot 3.4
- Spring RestClient
- JSch (SFTP)
- Apache Commons Net (FTP)
- Docker Multi-Stage Build
- Maven Wrapper
