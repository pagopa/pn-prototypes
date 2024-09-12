package it.pagopa.pn.splitcon020.svc.archivereader;

import it.pagopa.pn.splitcon020.utils.ZipInputStreamReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public abstract class AbstractCon020ArchiveReader {

    private final ZipInputStreamReader binReader;

    public AbstractCon020ArchiveReader() {
        ZipInputStreamReader<ParsingContext> p7mReader = new ZipInputStreamReader<>();
        p7mReader.addListener("PDF", (entry, inStrm, ctx ) -> {
            try {
                String fileKey = this.uploadPdfToSafeStorage( entry.getName(), inStrm.readAllBytes(), ctx.archiveId );
                ctx.p7mEntryName2safeStorageKey.put( entry.getName(), fileKey);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        p7mReader.addListener("BOL", (entry, inStrm, ctx ) -> {
            try {
                this.parseBol( entry.getName(), inStrm.readAllBytes(), ctx );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        ZipInputStreamReader<ParsingContext> binReader = new ZipInputStreamReader<>();
        binReader.addListener("p7m", (entry, inStrm, ctx ) -> {
            p7mReader.readStream( inStrm, ctx);
        });

        this.binReader = binReader;
    }

    protected abstract String uploadPdfToSafeStorage(String name, byte[] pdfBytes, String archiveId);

    private void parseBol(String name, byte[] bolBytes, ParsingContext ctx) {
        String bolString = new String( bolBytes );

        for( String line : bolString.split("\n")) {
            if( line.length() > 0) {
                String[] cells = line.split("\\|");
                String p7mEntryName = cells[0];
                String requestId = cells[3];
                String registeredLetterCode = cells[6];

                if( p7mEntryName.toLowerCase().endsWith("pdf")) {
                    List<String> key = buildKey( ctx.archiveId, requestId, registeredLetterCode );
                    ctx.bolLineId2p7mEntryName.put( key, p7mEntryName );
                }
            }
        }
    }

    private static List<String> buildKey(String archiveId, String requestId, String registeredLetterCode ) {
        return new ArrayList<>(Arrays.asList( archiveId, requestId, registeredLetterCode ));
    }

    public Con020ArchiveProcessingResult readStream( InputStream inStrm, String archiveId ) {
        ParsingContext ctx = new ParsingContext( archiveId );
        this.binReader.readStream( inStrm, ctx );
        return ctx.toResults();
    }

    private static class ParsingContext {
        private final Map<List<String>, String> bolLineId2p7mEntryName = new HashMap<>();

        private final Map<String, String> p7mEntryName2safeStorageKey = new HashMap<>();

        private final String archiveId;

        private ParsingContext(String archiveId) {
            this.archiveId = archiveId;
        }

        private Con020ArchiveProcessingResult toResults() {
            List<Con020ArchiveProcessingResult.Entry> entries = new ArrayList<>();

            for( Map.Entry<List<String>, String> bolEntry: bolLineId2p7mEntryName.entrySet() ) {
                List<String> lineId = bolEntry.getKey();
                String archiveId = lineId.get( 0 );
                String requestId = lineId.get( 1 );
                String registeredLetterCode = lineId.get( 2 );
                String safeStorageKey = p7mEntryName2safeStorageKey.get( bolEntry.getValue() );

                Con020ArchiveProcessingResult.Entry resultEntry = new Con020ArchiveProcessingResult.Entry(
                        archiveId,
                        requestId,
                        registeredLetterCode,
                        safeStorageKey
                    );
                entries.add( resultEntry );
            }

            return new Con020ArchiveProcessingResult( entries );
        }
    }

}
