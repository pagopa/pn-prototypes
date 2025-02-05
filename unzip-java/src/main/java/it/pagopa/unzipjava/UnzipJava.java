package it.pagopa.unzipjava;

import java.io.*;
import java.util.zip.*;

public class UnzipJava {
    public static void unzip(String zipFilePath, String destDirectory) throws IOException {
        File destDir = new File(destDirectory);
        if (!destDir.exists()) {
            destDir.mkdirs();
        }
        try (FileInputStream fis = new FileInputStream(zipFilePath);
             ZipInputStream zis = new ZipInputStream(fis)) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                File newFile = new File(destDirectory, entry.getName());
                if (entry.isDirectory()) {
                    newFile.mkdirs();
                } else {
                    File parent = newFile.getParentFile();
                    if (!parent.exists()) {
                        parent.mkdirs();
                    }
                    try (FileOutputStream fos = new FileOutputStream(newFile)) {
                        byte[] buffer = new byte[4096];
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                    }
                }
                zis.closeEntry();
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Uso: java UnzipJava <file.zip> <directory_destinazione>");
            return;
        }
        String zipFilePath = args[0];
        String destDirectory = args[1];
        try {
            unzip(zipFilePath, destDirectory);
            System.out.println("File estratti con successo in: " + destDirectory);
        } catch (IOException e) {
            System.err.println("Errore durante l'estrazione: " + e.getMessage());
        }
    }
}

