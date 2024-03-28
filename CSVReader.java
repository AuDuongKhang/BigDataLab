import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CSVReader {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: CSVReader <inputCsvFile> <outputTextFile>");
            System.exit(1);
        }

        // Initialize Hadoop configuration and FileSystem
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Path to the CSV file (provided as command-line argument)
        Path csvFilePath = new Path(args[0]);

        // Path to the output file in HDFS (provided as command-line argument)
        Path outputPath = new Path(args[1]);

        // Open an InputStream for the CSV file
        try (InputStream inputStream = fs.open(csvFilePath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath)))) {
            // Read and discard the first line (headers)
            reader.readLine();

	    // Read and process each line of the CSV file
            String line;
            while ((line = reader.readLine()) != null) {
                // Split the line into fields using comma as delimiter
                String[] fields = line.split(",");
                
                // Assuming the first column is the class and the rest are features
                String classLabel = fields[0].trim();
                double x1 = Double.parseDouble(fields[1].trim());
                double x2 = Double.parseDouble(fields[2].trim());

                // Write the data to the output text file in HDFS
                writer.write(String.format("%.9f", x1) + " " + String.format("%.9f", x2));
                writer.newLine(); // Add a new line
            }
        }
    }
}


