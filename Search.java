package org.myorg;


// Rakesh Harish 800984018

// This program will accept through the command line a space separated user query.
// The user arguments are for input path and output path respectively.
// The output files of the TFIDF.java would be the input for the mapper

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Search extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(Search.class);

	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Search(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {	
		
		Job job = Job.getInstance(getConf(), " search "); //creates the hadoop job
		
		
		String[] totalArgs = new String[args.length - 2]; //create array to get user query 
		for(int i = 2; i < args.length; i++){
			totalArgs[i-2] = args[i].toLowerCase(); // convert user query to lower case alphabets
		}

		job.getConfiguration().setStrings("totalArgs",totalArgs); // (*)query saved to configuration for later usage in mapper
		
		job.setJarByClass(this.getClass()); 
		FileInputFormat.addInputPaths(job, args[0]); //HDFS input path; the same should be TFIDF.java's output path
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // HDFS output path
		job.setMapperClass(Map.class); // Mapper class 
		job.setReducerClass(Reduce.class); // Reducer class
		job.setOutputKeyClass(Text.class); // The Key-value pair; key definition
		job.setOutputValueClass(Text.class); // The Key-value pair; value definition

		return job.waitForCompletion(true) ? 0 : 1; // returns true if its successful with all statistics
	}

	public static class Map extends
 Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();


		List<String> queryWords; 
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			String[] totalArgs = context.getConfiguration().getStrings("totalArgs");
			queryWords = new ArrayList<String>();
			queryWords =  (List) Arrays.asList(totalArgs); //(*)saved query accessed and made as array
			
		}

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {

			String line = lineText.toString();
			Text filename = new Text();
			Text tfidf = new Text();
			if (line.isEmpty()) {
				return;
			}
			String[] eachline = line.toString().split("#####"); // the line is split by the delimeter
			if (eachline.length < 1) {
				return;
			}
			if(queryWords.contains(eachline[0])){ // if statement is to see if any words in query is present in the line to satisfy the condition

				String[] filenameTFIDF = eachline[1].split("\\s+"); // matched file name is accessed with its TFIDF
				filename = new Text(filenameTFIDF[0]);
				tfidf = new Text(filenameTFIDF[1]);
				context.write(filename, tfidf); // write file name and TFIDF as key value pair
				
			}
	
		}
	}

	public static class Reduce extends
 Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text word, Iterable<Text> counts,
				Context context) throws IOException, InterruptedException {
			double sum = 0;
			
			int c=0;
			for (Text count : counts) {
				c++;
		        	sum += Double.parseDouble(count.toString()); // sum of TFIDF is calculated
		  	}
		    	Text combinedTfidf = new Text(sum + "");
		    	context.write(word, combinedTfidf);
		}
	}	

}
