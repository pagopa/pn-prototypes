package it.pagopa.pn.splitcon020.utils;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class ZipInputStreamReader<C> {

    private final Map<String, TriConsumer< ArchiveEntry, InputStream, C>> listeners;

    public ZipInputStreamReader() {
        this.listeners = new HashMap<>();
    }

    public void addListener( String lastDotSuffix, TriConsumer<ArchiveEntry, InputStream, C> listener) {
        this.listeners.put( lastDotSuffix.toLowerCase(), listener);
    }

    public void readStream( InputStream inStrm, C ctx ) {

        ArchiveEntry ze;

        try(ArchiveInputStream zipIn = new ZipArchiveInputStream( inStrm )) {

            while ((ze = zipIn.getNextEntry()) != null) {

                String entryName = ze.getName();

                int lastDotPosition = entryName.lastIndexOf('.');
                if( lastDotPosition > 0 ) {
                    String exstension = entryName.substring( lastDotPosition + 1)
                            .toLowerCase();

                    TriConsumer<ArchiveEntry, InputStream, C> listener = this.listeners.get( exstension );

                    if( listener != null ) {
                        listener.accept( ze, zipIn, ctx);
                    }
                }

            }
        } catch (IOException exc) {
            throw new RuntimeException( exc );
        }

    }
}
