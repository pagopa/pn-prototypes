package it.pagopa.pn.splitcon020;

import it.pagopa.pn.splitcon020.svc.archivereader.AbstractCon020ArchiveReader;
import it.pagopa.pn.splitcon020.svc.archivereader.Con020ArchiveProcessingResult;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class Con020BinaryReaderTest {

    @Test
    public void oneTest() {

        AbstractCon020ArchiveReader reader = new AbstractCon020ArchiveReader() {

            @Override
            protected String uploadPdfToSafeStorage(String name, byte[] pdfBytes, String archiveId) {
                return "safestorage://" + archiveId + "/" + name;
            }
        };

        String url = "file:///Users/mvit/Downloads/pippo/PN_EXTERNAL_LEGAL_FACTS.zip";

        try( InputStream inStrm = openUrl( url )) {
            Con020ArchiveProcessingResult result = reader.readStream( inStrm, url );

            for( Con020ArchiveProcessingResult.Entry entry: result.getEntries() ) {
                System.out.println( entry );
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static InputStream openUrl( String url ) {
        try {
            return new URL( url ).openStream();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void pippo() {
        String a = "PREPARE_SIMPLE_REGISTERED_LETTER.IUN_NATN-YGVJ-YXAG-202402-R-1.RECINDEX_0.PCRETRY_1";
        String iun = a.replaceFirst(".*\\.IUN_([^\\.]*)\\..*", "$1");
        System.out.println( iun );
    }
}
