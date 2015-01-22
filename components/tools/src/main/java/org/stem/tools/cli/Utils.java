/*
 * Copyright 2015 Alexey Plotnik
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.stem.tools.cli;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Utils {
    public static void printLine(String message) {
        System.out.println(message);
    }

    public static void printLine(String message, Object... params) {
        System.out.println(String.format(message, params));
    }

    public static void printLine() {
        System.out.println();
    }

    /**
     * Save data to file
     *
     * @param blob binary data
     * @param fileName file to save result to
     * @throws java.io.IOException
     */
    public static void writeToFile(byte[] blob, String fileName) throws IOException {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(fileName);
            fos.write(blob);
            fos.flush();
        } catch (IOException ex) {
            throw new IOException(ex.getMessage());
        } finally {
            if (fos != null)
                fos.close();
        }
    }

    /**
     * Get data from file
     *
     * @param fileName file name to read commands from
     * @return byte[] binary data
     * @throws IOException
     */
    public static byte[] readFromFile(String fileName, final int maxSize) throws IOException {
        FileInputStream fis = null;
        Path filePath = Paths.get(fileName);
        if (!Files.exists(filePath) && !Files.isRegularFile(filePath)) {
            throw new FileNotFoundException("There is no file or it is not regular file.");
        }

        if (Files.size(filePath) > maxSize) {
            throw new IOException("File is too big");
        }
        byte[] blob = new byte[(int) Files.size(filePath)];

        try {
            fis = new FileInputStream(fileName);
            return blob;
        } catch (IOException ex) {
            throw new IOException(ex.getMessage());
        } finally {
            if (fis != null)
                fis.close();
        }

    }
}
