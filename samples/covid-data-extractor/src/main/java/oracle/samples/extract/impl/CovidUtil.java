package oracle.samples.extract.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.zip.ZipEntry;

public class CovidUtil {
    private static final Logger logger = LoggerFactory.getLogger(CovidUtil.class);
    public static final String EXTRACT_OUTPUT_DIR = "EXTRACT_OUTPUT_DIR";


    public static void putAllMap(Properties props, Map<String, ?> map) {
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            props.put(entry.getKey(), entry.getValue());
        }
    }

    public static void free(AutoCloseable resource) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Throwable t) {
            logger.error("Unable to close resource", t);
        }
    }

    // protect zip slip attack
    public static Path zipSlipProtect(ZipEntry zipEntry, Path targetDir) throws IOException {
        Path targetDirResolved = targetDir.resolve(zipEntry.getName());
        // make sure normalized file still has targetDir as its prefix
        // else throws exception
        Path normalizePath = targetDirResolved.normalize();
        if (!normalizePath.startsWith(targetDir)) {
            throw new IOException("zipSlipProtect : Bad zip entry: " + zipEntry.getName());
        }
        return normalizePath;
    }

}
