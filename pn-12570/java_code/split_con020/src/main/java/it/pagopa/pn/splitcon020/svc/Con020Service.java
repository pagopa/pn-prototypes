package it.pagopa.pn.splitcon020.svc;

import it.pagopa.pn.commons.exceptions.PnRuntimeException;
import it.pagopa.pn.splitcon020.svc.archivereader.AbstractCon020ArchiveReader;
import it.pagopa.pn.splitcon020.svc.archivereader.Con020ArchiveProcessingResult;
import it.pagopa.pn.splitcon020.dao.Con020ArchiveEntityDAO;
import it.pagopa.pn.splitcon020.dao.Con020EnrichedEntityDAO;
import it.pagopa.pn.splitcon020.dto.Con020InputEventDto;
import it.pagopa.pn.splitcon020.dto.Con020NewArchiveDto;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

@Service
public class Con020Service {

    private final Con020EnrichedEntityDAO con020EvtDao;
    private final Con020ArchiveEntityDAO con020ArcDao;

    private final SafeStorageClientService safeStorage;

    public Con020Service(Con020EnrichedEntityDAO con020EvtDao, Con020ArchiveEntityDAO con020ArcDao, SafeStorageClientService safeStorage) {
        this.con020EvtDao = con020EvtDao;
        this.con020ArcDao = con020ArcDao;
        this.safeStorage = safeStorage;
    }

    public void receiveInputEvent( Con020InputEventDto con020EventFromExtCh ) {
        System.out.println( "CON020 INPUT EVENT " + con020EventFromExtCh );
        con020EvtDao.saveInputEventMetadata( con020EventFromExtCh );
        con020ArcDao.saveArchiveIfNotExsists( con020EventFromExtCh.getAttachmentFileKey() );
    }

    public void startArchiveProcessing( Con020NewArchiveDto con020NewArchive ) {
        System.out.println( "CON020 NEW ARCHIVE " + con020NewArchive );
        String archiveFileKey = con020NewArchive.getArchiveFileKey();

        con020ArcDao.updateStatus( con020NewArchive.getArchiveFileKey(), "PROCESSING" );
        try( InputStream con020ArchiveInStrm = safeStorage.download( archiveFileKey )) {


            Con020ArchiveReader reader = new Con020ArchiveReader();
            Con020ArchiveProcessingResult result = reader.readStream( con020ArchiveInStrm, archiveFileKey );

            for( Con020ArchiveProcessingResult.Entry entry: result.getEntries() ) {
                this.safeStorage.changeStatusToPdf( entry.getPdfSafeStorageFileKey(), "ATTACHED" );
                this.con020EvtDao.writePdfUriIntoCon020EventEntity(
                        entry.getArchiveId(),
                        entry.getRequestId(),
                        entry.getRegisteredLetterCode(),
                        entry.getPdfSafeStorageFileKey()
                    );
            }
        }
        catch (IOException exc) {
            throw new PnRuntimeException( "IOEXCEPTION", exc.getMessage(), 500, Collections.emptyList(), exc );
        }

        con020ArcDao.updateStatus( con020NewArchive.getArchiveFileKey(), "PROCESSED" );
    }


    private class Con020ArchiveReader extends AbstractCon020ArchiveReader {
        @Override
        protected String uploadPdfToSafeStorage(String name, byte[] pdfBytes, String archiveId) {
            return Con020Service.this.safeStorage.uploadPdf( pdfBytes );
        }
    }

}
