package com.couchbase.lite.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Created by hideki on 8/27/15.
 */
public class ZipUtils {
    public static void unzip(InputStream in, File destination) throws IOException {

        ZipInputStream zis = new ZipInputStream(in);
        try {
            ZipEntry ze = zis.getNextEntry();
            while (ze != null) {
                String fileName = ze.getName();
                File newFile = new File(destination, fileName);
                if (ze.isDirectory()) {
                    newFile.mkdirs();
                } else {
                    new File(newFile.getParent()).mkdirs();
                    FileOutputStream fos = new FileOutputStream(newFile);
                    try {
                        byte[] buffer = new byte[1024];
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                    }finally {
                        fos.close();
                    }
                }
                ze = zis.getNextEntry();
            }
            zis.closeEntry();
        }finally{
            try {
                zis.close();
            } catch (IOException e) {
            }
            try {
                in.close();
            } catch (IOException e) {
            }
        }
    }
}
