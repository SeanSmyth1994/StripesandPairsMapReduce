package wordpair;

import java.io.DataInput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordPairMapReduce {
	public static class WordPairMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		IntWritable wordpairValue = new IntWritable(1);
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = clean(value.toString());
			String second = new String();
			String first = new String();
			StringTokenizer tokenizer = new StringTokenizer(line);
			// Initialize 
			if (tokenizer.hasMoreTokens()) {
				first = tokenizer.nextToken();
				second = first;
			}

			// concatenate first word with second word and write it to disk with it's subsequent word.
			// this works much like word count
			while (tokenizer.hasMoreTokens()) {
				first = second;
				second = tokenizer.nextToken();
				
				first = first + " " + second;

				Text WordPair = new Text(first.toLowerCase());
				
				context.write(WordPair, wordpairValue);
				// end of line
				if (!tokenizer.hasMoreTokens()) {
					WordPair.set(second.toLowerCase());
					context.write(WordPair, wordpairValue);
				}
			}
		}

		public static String clean(String word) {
			Pattern pattern = Pattern.compile("[^0-9 a-z A-Z]");
			Matcher matcher = pattern.matcher(word);
			String newWord = matcher.replaceAll(" ");
			return newWord;
		}
	}

	public static class WordPairReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text word, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(word, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Pairs Approach");
		job.setJarByClass(WordPairMapReduce.class);
		job.setMapperClass(WordPairMapper.class);
		//job.setCombinerClass(WordPairReducer.class);
		job.setReducerClass(WordPairReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
		
	}

}
