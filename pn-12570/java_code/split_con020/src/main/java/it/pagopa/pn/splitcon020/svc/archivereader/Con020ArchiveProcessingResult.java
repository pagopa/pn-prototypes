package it.pagopa.pn.splitcon020.svc.archivereader;

import lombok.Data;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Con020ArchiveProcessingResult {

    private final List<Entry> entries;

    public Con020ArchiveProcessingResult( List<Entry> entries ) {
        this.entries = Collections.unmodifiableList( new ArrayList<>( entries ));
    }

    public List<Entry> getEntries() {
        return entries;
    }

    @Data
    @ToString
    public static class Entry {

        private final String archiveId;
        private final String requestId;
        private final String registeredLetterCode;

        private final String pdfSafeStorageFileKey;
    }
}
