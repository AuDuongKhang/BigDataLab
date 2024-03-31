import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Collections;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HighestAverage {
    private static Map<String, Integer> termIdMap;
    private static Map<String, Integer> docIdMap;

    public static class AverageMapper extends Mapper<LongWritable, Text, Text, Text> {
	
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			Path docIdMapPath = new Path(conf.get("input") + "/" + "docIdMap.txt");
			Path termIdMapPath = new Path(conf.get("input") + "/" + "termIdMap.txt");
			docIdMap = new HashMap<>();
			termIdMap = new HashMap<>();

			try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(docIdMapPath)))) {
				String line;
				while ((line = br.readLine()) != null) {
					String[] parts = line.split(",");
					if (parts.length == 2) {
						String key = parts[0];
					int value = Integer.parseInt(parts[1]);
					docIdMap.put(key, value);
					} 
				}
			}
			
			try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(termIdMapPath)))) {
				String line;
				while ((line = br.readLine()) != null) {
					String[] parts = line.split(",");
					if (parts.length == 2) {
						String key = parts[0];
						int value = Integer.parseInt(parts[1]);
						termIdMap.put(key, value);
					}
				}
			}
		}

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\t");
			String docId = parts[1];
			Text tfidf = new Text();
			tfidf.set(parts[2]);
			String fullDocName = getFullDocName(Integer.parseInt(docId), docIdMap);
			String termId = parts[0].trim();
			String termName = getTermName(Integer.parseInt(termId), termIdMap);
			Text className = new Text (fullDocName.split("\\.")[0]);

            context.write(new Text(className), new Text(termName + " " + tfidf));
        }
    }

	public static class AverageReducer extends Reducer<Text, Text, NullWritable, Text> {
		// For choosing |C_i| = sum of documents in class i
		//Map<String, Integer> categoryDocCount;
		private BufferedWriter bw;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			Path outputPath = new Path (conf.get("output_avg") + "/task_1_5.txt");
			bw = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath)));
		    // For choosing |C_i| = sum of documents in class i
		   /* Path categoryDocCountPath = new Path(conf.get("input") + "/" + "categoryDocCount.txt");
		    categoryDocCount = new HashMap<>();

		    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(categoryDocCountPath)))) {
				String line;
				while ((line = br.readLine()) != null) {
					String[] parts = line.split(",");
					if (parts.length == 2) {
						String key = parts[0];
						int value = Integer.parseInt(parts[1]);
						categoryDocCount.put(key, value);
	                }
				}
			}*/
	    }

		private static final int TOP_N = 5;

	   	@Override
		protected void reduce(Text className, Iterable<Text> termIdTfidfValues, Context context)
	        throws IOException, InterruptedException {
	    // Map to store the sum of TF-IDF scores and count of terms
	    Map<String, Double> termSum = new HashMap<>();
	    Map<String, Integer> termCount = new HashMap<>();
	    TreeMap<Double, String> termAvgMap = new TreeMap<>(Collections.reverseOrder());

	    // Iterate through all the TF-IDF values for the term
	    for (Text termIdTfidfValue : termIdTfidfValues) {
	        String[] parts = termIdTfidfValue.toString().split(" ");
	        if (parts.length == 2) {
	            String termId = parts[0];
	            double tfidf = Double.parseDouble(parts[1]);

	            // Update sum and count for the term
	            termSum.put(termId, termSum.getOrDefault(termId, 0.0) + tfidf);
	            termCount.put(termId, termCount.getOrDefault(termId, 0) + 1);
	        }
	    }

	    // Compute the average TF-IDF score for each term
	    for (Map.Entry<String, Double> entry : termSum.entrySet()) {
	        String termId = entry.getKey();
	        double sum = entry.getValue();
	        int count = termCount.getOrDefault(termId, 0);
		/*
		// For choosing |C_i| = sum of documents in class i
		for (Map.Entry<String, Integer> entry_2 : categoryDocCount.entrySet()) {
		    if (entry_2.getKey().equals(className.toString())) {
				count = entry_2.getValue(); 
			}
		}
		*/
	        double avg = sum / count;
	        // Store the average TF-IDF score in the TreeMap
	        termAvgMap.put(avg, termId);
	    }

	    // Output the top 5 terms with the highest average TF-IDF scores
	    StringBuilder topTerms = new StringBuilder();
	    int count = 0;
	    topTerms.append(className.toString() + ": ");
		
	    for (Map.Entry<Double, String> entry : termAvgMap.entrySet()) {
	        if (count >= TOP_N) {
	            break;
	        }
	        topTerms.append(entry.getValue()).append(":").append(String.format("%.2f", entry.getKey())).append(",");
	        count++;
	    }
		
	    if (topTerms.length() > 0 && topTerms.charAt(topTerms.length() - 1) == ',') {
    		topTerms.deleteCharAt(topTerms.length() - 1);
	    }	

	    // Write the top 5 terms for the class to the output
	    context.write(NullWritable.get(), new Text(topTerms.toString()));
	    bw.write(topTerms.toString() + ": ");
		}
	     @Override
             protected void cleanup(Context context) throws IOException, InterruptedException {
            	bw.close(); // Close the buffered writer to release resources
             }
    }

    public static void main(String[] args) throws Exception {
        // Configure and run the MapReduce job
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        String termsFile = "bbc.terms";
        String docsFile = "bbc.docs";

        conf.set("input", args[0]);
        conf.set("output_avg", args[1]);

		if (fs.exists(new Path(conf.get("output_avg")))) {
            fs.delete(new Path(conf.get("output_avg")), true);
        }

        Path docsPath = new Path(conf.get("input") + "/" + docsFile);
        Path termPath = new Path(conf.get("input") + "/" + termsFile);
		String docIdMapPath = conf.get("input") + "/docIdMap.txt";
		String termIdMapPath = conf.get("input") + "/termIdMap.txt";
		String categoryDocCountPath = conf.get("input") + "/categoryDocCount.txt";

        termIdMap = readTermIds(termPath, fs);
        docIdMap = readDocIds(docsPath, fs);
		
		Map<String,Integer> categoryDocCount =  numberOfDocEachCategory(docIdMap);
		saveDocIdMapToHDFS(docIdMap, docIdMapPath);
		saveTermIdMapToHDFS(termIdMap, termIdMapPath);
		saveCategoryCountToHdfs(categoryDocCount, categoryDocCountPath);

        Job job = Job.getInstance(conf, "Average TF-IDF per Term per Class");
        job.setJarByClass(HighestAverage.class);
        job.setMapperClass(AverageMapper.class);
        job.setReducerClass(AverageReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0] + "/task_1_4.mtx"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
	
	public static void saveDocIdMapToHDFS(Map<String, Integer> docIdMap, String filePath) {
        try {
            // Initialize Hadoop configuration
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            
            // Create output path in HDFS
            Path outputPath = new Path(filePath);
            
            // Open output stream to HDFS file
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath, true)));

            // Write docIdMap entries to the file
            for (Map.Entry<String, Integer> entry : docIdMap.entrySet()) {
                writer.write(entry.getKey() + "," + entry.getValue() + "\n");
            }

            // Close the writer and file system
            writer.close();
            fs.close();

            System.out.println("DocIdMap saved to HDFS successfully.");
        } catch (Exception e) {
            System.err.println("Error saving DocIdMap to HDFS: " + e.getMessage());
            e.printStackTrace();
        }
    }
	
 	public static void saveTermIdMapToHDFS(Map<String, Integer> termIdMap, String filePath) {
        try {
            // Initialize Hadoop configuration
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            // Create output path in HDFS
            Path outputPath = new Path(filePath);

            // Open output stream to HDFS file
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath, true)));

            // Write docIdMap entries to the file
            for (Map.Entry<String, Integer> entry : termIdMap.entrySet()) {
                writer.write(entry.getKey() + "," + entry.getValue() + "\n");
            }

            // Close the writer and file system
            writer.close();
            fs.close();

            System.out.println("TermIdMap saved to HDFS successfully.");
        } catch (Exception e) {
            System.err.println("Error saving TermIdMap to HDFS: " + e.getMessage());
            e.printStackTrace();
        }
	}
	
	public static void saveCategoryCountToHdfs(Map<String, Integer> categoryCount, String filePath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path(filePath);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath, true)));
		// Write docIdMap entries to the file
		for (Map.Entry<String, Integer> entry : categoryCount.entrySet()) {
			writer.write(entry.getKey() + "," + entry.getValue() + "\n");
		}

        // Close the writer and file system
        writer.close();
		fs.close(); 
    }

    private static Map<String, Integer> readTermIds(Path termPath, FileSystem fs) throws IOException {
		Map<String, Integer> termIdMapTemp = new HashMap<>();

		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(termPath)));
		String line;
		int id = 1;
		while ((line = br.readLine()) != null) {
			termIdMapTemp.put(line.trim(), id++);
		}
		br.close(); // Close the BufferedReader explicitly

		return termIdMapTemp;
	}


    private static Map<String, Integer> readDocIds(Path docsPath, FileSystem fs) throws IOException {
		Map<String, Integer> docIdMapTemp = new HashMap<>();

		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(docsPath)));
		String line;
		int id = 1;
		while ((line = br.readLine()) != null) {
			String[] parts = line.split("\\.");
			if (parts.length == 2) {
				String category = parts[0];
				String docName = parts[1];
				String fullDocName = category + "." + docName;
				docIdMapTemp.put(fullDocName, id++);
			}
		}
		br.close(); // Close the BufferedReader explicitly
		return docIdMapTemp;
	}

	private static Map<String, Integer> numberOfDocEachCategory(Map<String, Integer> docIdMap) throws IOException {
		Map<String, Integer> categoryCount = new HashMap<>();

		for (Map.Entry<String, Integer> entry : docIdMap.entrySet()) {
			// Extract category from the key
			String category = getCategoryFromKey(entry.getKey());

			// Increment count for the category
			categoryCount.put(category, categoryCount.getOrDefault(category, 0) + 1);
		}
		return categoryCount;
	}

	private static String getCategoryFromKey(String key) {
		// Extract category from the key (assuming the format is "category.docname")
		String[] parts = key.split("\\.");
		if (parts.length > 0) {
			return parts[0];
		}
		return null;
	}


    private static String getFullDocName(int docId, Map<String, Integer> docIdMap) {
		for (Map.Entry<String, Integer> entry : docIdMap.entrySet()) {
			if (entry.getValue() == docId) {
			return entry.getKey();
			}
		}
			return null;
	}
	
	private static String getTermName(int termId, Map<String, Integer> termIdMap) {
		for (Map.Entry<String, Integer> entry : termIdMap.entrySet()) {
			if (entry.getValue() == termId) {
			   return entry.getKey();
			}
		}
		return null;
	}
}

