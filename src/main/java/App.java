import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by cjemison on 10/29/17.
 */
public class App {

    public static void main(String[] args) throws Exception {
        deleteDir(new File("/tmp/chunk"));
        final App app = new App();
        // creates a file with integers.
        app.generateFile(100000000);
        // chunks up the data using streams.
        app.chunkFile(1000000);
        // emulating merge sort over files.
        app.sort();
    }

    public static boolean deleteDir(File dir) throws IOException {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            for (int i = 0; i < files.length; i++) {
                Files.delete(Paths.get(files[i].getAbsolutePath()));
            }
        }
        return dir.delete();
    }

    private void generateFile(final int num) throws IOException {
        final Random random = new Random();
        final PrintWriter printWriter = new PrintWriter(new BufferedWriter(new FileWriter(new File("/tmp/data.txt"))));
        for (int i = 0; i < num; i++) {
            printWriter.println(random.nextInt());
            if (i % 100000 == 0) {
                System.out.println(i);
                printWriter.flush();
            }
        }
        printWriter.flush();
        printWriter.close();
    }

    private void chunkFile(final int chunkSize) throws IOException {
        File f = new File("/tmp/chunk");
        if (!f.exists()) {
            f.mkdir();
        } else {
            deleteDir(f);
        }

        int chunk = 0;
        int cnt = 1;
        final BufferedReader reader = new BufferedReader(new FileReader(new File("/tmp/data.txt")));
        String line;
        PrintWriter printWriter = new PrintWriter(new BufferedWriter(new FileWriter(new File(String.format("/tmp/chunk/chunk_%d.txt", chunk)))));
        while ((line = reader.readLine()) != null) {
            printWriter.println(line);
            if (cnt % chunkSize == 0) {
                chunk++;
                System.out.println(String.format("Chunk: %d", chunk));
                printWriter.flush();
                printWriter.close();
                printWriter = new PrintWriter(new BufferedWriter(new FileWriter(new File(String.format("/tmp/chunk/chunk_%d.txt", chunk)))));
            }
            cnt++;
            //break;
        }
        if (printWriter != null) {
            printWriter.flush();
            printWriter.close();
        }
        reader.close();
        Files.delete(Paths.get("/tmp/data.txt"));
    }

    private void sort() throws Exception {
        Files.deleteIfExists(Paths.get("/tmp/sorted.txt"));
        File f = new File("/tmp/sorted.txt");
        if (!f.exists()) {
            new FileOutputStream(f).close();
        }
        f.setLastModified(System.currentTimeMillis());

        File folder = new File("/tmp/chunk");
        final Set<String> files = Arrays.stream(folder.listFiles()).map(File::getName).sorted(String::compareTo).collect(Collectors.toSet());
        // using map reduce instead of threads
        files.stream().reduce((fileName1, fileName2) -> {
            try {
                if (!fileName1.equals("/tmp/sorted.txt")) {
                    processFile(fileName1);
                }
                processFile(fileName2);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return "/tmp/sorted.txt";
        });
    }

    private void processFile(final String fileName) throws Exception {
        System.out.println(String.format("Filename: %s", fileName));
        if (!fileName.trim().equals("")) {
            sortFile(String.format("/tmp/chunk/%s", fileName), "/tmp/sorted.txt");
        }
        sortFile("/tmp/sorted.txt", "/tmp/tmp.txt");
        Files.deleteIfExists(Paths.get("/tmp/sorted.txt"));
        Files.deleteIfExists(Paths.get(String.format("/tmp/chunk/%s", fileName)));
        File rename = new File("/tmp/tmp.txt");
        rename.renameTo(new File("/tmp/sorted.txt"));
    }

    private void sortFile(final String readFile, final String writeFile) throws Exception {
        final PrintWriter printWriter = new PrintWriter(new BufferedWriter(new FileWriter(writeFile, true)));
        try (Stream<String> stream = Files.lines(Paths.get(readFile))) {
            stream.map(Integer::valueOf).sorted(Integer::compareTo).forEach(printWriter::println);
        } catch (IOException e) {
            e.printStackTrace();
        }
        printWriter.close();
    }
}
