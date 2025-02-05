# UnzipJava

## Descrizione
UnzipJava è un'applicazione Java che consente di estrarre file ZIP in una directory specificata.
Può essere eseguita direttamente tramite il JAR o utilizzando Docker.

## Requisiti
- Java 17 o superiore
- Docker (opzionale per esecuzione containerizzata)

## Compilazione del progetto
Per compilare l'applicazione ed ottenere il file JAR eseguibile, eseguire dalla root del progetto:

```sh
./mvnw clean package
```

Il JAR verrà generato nella cartella `target/` con il nome:

```
target/unzip-java-1.0-SNAPSHOT.jar
```

## Esecuzione con JAR
Per eseguire l'applicazione direttamente tramite il JAR:

```sh
java -jar target/unzip-java-1.0-SNAPSHOT.jar <file.zip> <directory_destinazione>
```

Esempio:

```sh
java -jar target/unzip-java-1.0-SNAPSHOT.jar ~/Downloads/Archivio.zip ~/Downloads/output
```

## Esecuzione con Docker Compose
Nel file Docker Compose, sostituire il path dello zip sorgente e il path della cartella di output:
```yaml
volumes:
  - ~/Downloads/Archivio.zip:/app/test.zip # sostituisci ~/Downloads/Archivio.zip
  - ~/Downloads/output:/app/output # sostituisci ~/Downloads/output
```
Eseguire il container con:

```sh
docker compose up --force-recreate -V && docker compose down
```

Nota: è necessario aver creato prima il JAR per eseguire l'applicazione containerizzata.