package org.myorg;

//Rakesh Harish 800984018

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.commons.logging.LogFactory;


public class DocWordCount extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(DocWordCount.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new DocWordCount(), args); // initialize the run function
		System.exit(res);
	}
	

	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), " docwordcount "); // hadoop job creation
		job.setJarByClass(this.getClass()); // jar file creation
		
		FileInputFormat.addInputPaths(job, args[0]); // Input file path in HDFS	
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output file path in HDFS
		job.setMapperClass(Map.class); // Mapper class initialization 
		job.setReducerClass(Reduce.class); // Reducer class initialization
		job.setOutputKeyClass(Text.class); // Key defintion - as String with Text.class
		job.setOutputValueClass(IntWritable.class); // The Key-Value pair - value defined as FloatWritable

		return job.waitForCompletion(true) ? 0 : 1; // returns true if its successful with all statistics
	}

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
				
		// LongWritable is the offset to determine the line in the document
		// Text is the value for the entire document(line)
		// 3rd parameter the out key(text)
		// 4th parameter is the associated value to it 
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private static final Pattern WORD_BOUNDARY = Pattern
				.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			
			String line = lineText.toString();
			Text currentWord = new Text();
			for (String word : WORD_BOUNDARY.split(line)) {
				if (word.isEmpty()) {
					continue;
				}
				FileSplit filesplit = (FileSplit)context.getInputSplit();
                        	String filename = filesplit.getPath().getName();
                        	String delimeter = new String("#####");
                        	String v = word.toString().trim().toLowerCase() + delimeter + filename + "\t";
				currentWord = new Text(v);
				context.write(currentWord, one);
				
				// Assigning all the different words with default value of 1 
				// (hadoop,1)
				
			}
		}
	}
//Rakesh Harish 800984018
	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
				
		//1st and 2nd parameter is the same datatype from the mapper
		//3rd and 4th parameter is the final output with the key - value pair

		@Override
		public void reduce(Text word, Iterable<IntWritable> counts,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();   //sums the count in each ocurrence
			}
			context.write(word, new IntWritable(sum));
		}
	}
}


