import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KMeans {
    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private List<Point> centroids = new ArrayList<>();

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            // Load centroids from file
            Path centroidsPath = new Path("/task_2_1_cluster/centroids.txt");
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centroidsPath)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    centroids.add(Point.fromString(line));
                }
            }
        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Point dataPoint = Point.fromString(value.toString());
            int closestCentroidIdx = 0;
            double minDistance = Double.MAX_VALUE;
            for (int i = 0; i < centroids.size(); i++) {
                double distance = Point.euclideanDistance(dataPoint, centroids.get(i));
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidIdx = i;
                }
            }
            context.write(new IntWritable(closestCentroidIdx), value);
        }
    }

    public static class KMeansReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<Point> points = new ArrayList<>();
            for (Text value : values) {
                points.add(Point.fromString(value.toString()));
            }

            Point centroid = Point.computeCentroid(points);

            // Output centroid coordinates only
            context.write(NullWritable.get(), new Text(centroid.toString()));
        }
    }

    // Final reducer class for cluster information including centroid and points
    public static class FinalKMeansReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<Point> points = new ArrayList<>();
            for (Text value : values) {
                points.add(Point.fromString(value.toString()));
            }
            Point centroid = Point.computeCentroid(points);

            // Output cluster information including centroid and points
            StringBuilder output = new StringBuilder();
            output.append("Cluster ").append(key.get()).append(":\n");
            output.append("Centroid: ").append(centroid).append("\n");
            output.append("Points:\n");

            for (Point point : points) {
                output.append(point).append("\n");
            }

            context.write(NullWritable.get(), new Text(output.toString()));
        }
    }

    public static class Point {
        private double x;
        private double y;

        public Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        public static Point fromString(String str) {
            String[] parts = str.split(" ");
            double x = Double.parseDouble(parts[0]);
            double y = Double.parseDouble(parts[1]);
            return new Point(x, y);
        }

        public static double euclideanDistance(Point p1, Point p2) {
            return Math.sqrt(Math.pow(p1.x - p2.x, 2) + Math.pow(p1.y - p2.y, 2));
        }

        public static Point computeCentroid(List<Point> points) {
            double sumX = 0;
            double sumY = 0;
            for (Point point : points) {
                sumX += point.x;
                sumY += point.y;
            }
            double avgX = sumX / points.size();
            double avgY = sumY / points.size();
            return new Point(avgX, avgY);
        }

        @Override
        public String toString() {
            return x + " " + y;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: CSVReader <inputCsvFile> <outputTextFile> k iterations");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("k", args[2]); // Set k value in configuration
        conf.set("iteration", args[3]); // Set itegration value in configuration

        // Read data points from input file
        List<Point> dataPoints = readDataPoints(args[0]);

        // Initialize centroids using random data points
        List<Point> centroids = initializeCentroids(Integer.parseInt(conf.get("k")), dataPoints);

        // Write centroid into file
        FileSystem fs = FileSystem.get(conf);
        Path centroidsPath = new Path("/task_2_1_cluster/centroids.txt");

        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(centroidsPath)))) {
            for (Point centroid : centroids) {
                bw.write(centroid.toString() + "\n");
            }
        }

        Job job = Job.getInstance(conf, "KMeans");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        for (int i = 0; i < Integer.parseInt(conf.get("iteration")) - 1; i++) {
            if (!fs.exists(new Path(args[1] + "/" + "_iter_" + (i + 1)))) {
                fs.mkdirs(new Path(args[1] + "/" + "_iter_" + (i + 1))); // Create the output directory if it doesn't
                                                                         // exist
            } else {
                TextOutputFormat.setOutputPath(job, new Path(args[1] + "/" + "_iter_" + (i + 1)));
                job.waitForCompletion(true);
                // Update centroids for the next iteration
                centroids = updateCentroids(new Path(args[1] + "/" + "_iter_" + (i + 1)), fs);
                // Write updated centroids to file
                try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(centroidsPath)))) {
                    for (Point centroid : centroids) {
                        bw.write(centroid.toString() + "\n");
                    }
                }
            }
        }
        // Set reducer back to original implementation
        job.setReducerClass(FinalKMeansReducer.class);

        // Set output path for the final iteration
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

    private static List<Point> initializeCentroids(int k, List<Point> dataPoints) {
        List<Point> centroids = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < k; i++) {
            int randomIndex = random.nextInt(dataPoints.size());
            centroids.add(dataPoints.get(randomIndex));
        }

        return centroids;
    }

    private static List<Point> readDataPoints(String inputFile) throws IOException {
        List<Point> dataPoints = new ArrayList<>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(inputFile);

        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputPath)))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\\s+"); // Split by whitespace
                double x = Double.parseDouble(parts[0]);
                double y = Double.parseDouble(parts[1]);
                dataPoints.add(new Point(x, y));
            }
        }

        return dataPoints;
    }

    private static List<Point> updateCentroids(Path outputDir, FileSystem fs) throws IOException {
        List<Point> centroids = new ArrayList<>();
        Path centroidsPath = new Path(outputDir, "part-r-00000");

        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centroidsPath)))) {
            String line;
            while ((line = br.readLine()) != null) {
                Point centroid = Point.fromString(line); // Assuming tab-separated values
                centroids.add(centroid);
            }
        }
        return centroids;
    }
}
