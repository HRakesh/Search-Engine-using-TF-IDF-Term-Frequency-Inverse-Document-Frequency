package org.myorg;


//Rakesh Harish 800984018

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import java.util.ArrayList;
import org.apache.hadoop.fs.ContentSummary;


public class TFIDF extends Configured implements Tool {

	private static final Logger LOG = Logger.getLogger(TFIDF.class);
	
	
	public static void main(String[] args) throws Exception {
		
		String[] argstemp = new String[args.length];
		
		for(int i=0;i<args.length;i++) //copy passed arguments
		{
			argstemp[i] = args[i];
		}
		
		argstemp[1] = argstemp[0]+"temp";   //temp is added to save the output of TF
		
		System.out.println("one");
		
		int res1 = ToolRunner.run(new TermFrequency(), argstemp); //TermFrequency.java is called
		
		
		System.out.println("two");
		
		int res = ToolRunner.run(new TFIDF(), args); // TFIDF
		System.exit(res);
	}
	
	public int run(String[] args) throws Exception {
		
		
		
		FileSystem fs = FileSystem.get(new Configuration());
		ContentSummary cs = fs.getContentSummary(new Path(args[0]));
		getConf().set("NoOfFiles", cs.getFileCount() + "");  //calculates the number of files in the input directory
		
				
		Job job = Job.getInstance(getConf(), " TFIDF "); //creates the hadoop job
		job.setJarByClass(this.getClass());
		
		FileInputFormat.addInputPaths(job, args[0] + "temp"); //TF's output will be input 
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // HDFS output
		job.setMapperClass(Map.class); // Mapper class 
		job.setReducerClass(Reduce.class); // Reducer class
		job.setOutputKeyClass(Text.class); // The Key-value pair; key definition
		job.setOutputValueClass(Text.class); // The Key-value pair; value definition

		return job.waitForCompletion(true) ? 0 : 1; // returns true if its successful with all statistics
	}

	
	public static class Map extends
 Mapper<LongWritable, Text, Text, Text> {
		
		private Text word = new Text();
	

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
	

			String line = lineText.toString();
			
			Text word  = new Text(line.split("#####")[0]); //splits the line by the delimeter '####' to get the word
            String value = line.split("#####")[1]; //gets the value after the split; file name and count
            String finalValue = value.split("\\s+")[0]+"="+value.split("\\s+")[1]; //removes space and joins file name and count with = sign
            Text finalvalueText = new Text(finalValue);

            context.write(word,finalvalueText); 
		}
	}	

	public static class Reduce extends
 Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text word, Iterable<Text> count,
				Context context) throws IOException, InterruptedException {
			long NoOfFiles = context.getConfiguration().getLong("NoOfFiles", 1);  //gets the no. of the files count by configuration
			
			ArrayList<Text> NoOfFilesWithWord = new ArrayList<Text>();
			for (Text filecountwithword : count) {
				NoOfFilesWithWord.add(new Text(filecountwithword.toString())); //adds the combination of filename and tf to array list
			}

			for (Text files : NoOfFilesWithWord) {
				String[] filenameWithTF = files.toString().split("="); //splits it by = to get the file name and TF separately
				double tfidf = 0;
				tfidf = Double.parseDouble(filenameWithTF[1])
						* Math.log10(1 + (NoOfFiles / NoOfFilesWithWord.size())); //calculation of tfidf
				String v = word.toString() + "#####" + filenameWithTF[0] + "\t";  //combines the word with the file name by adding the delimiter
				Text currentword = new Text(v);
				context.write(currentword, new Text(tfidf + ""));
			}
		}
	}

	
}
