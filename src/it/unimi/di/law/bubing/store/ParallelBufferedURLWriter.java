package it.unimi.di.law.bubing.store;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class ParallelBufferedURLWriter {
        private static File file;

        public static void SetURLFileName(String test) { file = new File(test); }

        public static void writeToFileBufferedWriter(String msg) {
            FileWriter fileWriter;
            BufferedWriter bufferedWriter;
            try {
                fileWriter = new FileWriter(file.getAbsoluteFile(), true); // true to append
                bufferedWriter = new BufferedWriter(fileWriter);
                bufferedWriter.write(msg);
                bufferedWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
}
