package WordPairPartitioner;

import java.io.DataInput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordPairMapReducewithPartitioner {
	public static class WordPairMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = clean(value.toString());
			String secondword = new String();
			String firstword = new String();

			StringTokenizer itr = new StringTokenizer(line);

			// Initializing the firstword and the secondword.
			if (itr.hasMoreTokens()) {
				firstword = itr.nextToken();
				secondword = firstword;
			}

			// At the start of each loop the secondword is put into the firstword and
			// concatenated with it's subsequent word.
			while (itr.hasMoreTokens()) {
				firstword = secondword;
				secondword = itr.nextToken();

				firstword = firstword + " " + secondword;

				Text word = new Text(firstword.toLowerCase());
				IntWritable ONE = new IntWritable(1);
				context.write(word, ONE);

				// At the end of each line it will only take the last word as key.
				if (!itr.hasMoreTokens()) {
					word.set(secondword.toLowerCase());
					context.write(word, ONE);
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

	public static class WordPairPartitioner extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text wordpair, IntWritable arg1, int arg2) {
			// TODO Auto-generated method stub
			String pair = wordpair.toString();
			if (arg2 == 0) {
				return 0;
			}
			// if we partition correctly we can save reducer time. A and E are the two most
			// popular letters in the alphabet...so lets split them up
			if (pair.startsWith("a") || pair.startsWith("b") || pair.startsWith("c") || pair.startsWith("d")
					|| pair.startsWith("f") || pair.startsWith("g") || pair.startsWith("h") || pair.startsWith("i")
					|| pair.startsWith("j") || pair.startsWith("k") || pair.startsWith("l") || pair.startsWith("m")) {
				return 0;
				// if the string starts with a number or e
			} else if (pair.startsWith("0") || pair.startsWith("e") || pair.startsWith("1") || pair.startsWith("2")
					|| pair.startsWith("3") || pair.startsWith("4") || pair.startsWith("5") || pair.startsWith("6")
					|| pair.startsWith("7") || pair.startsWith("8") || pair.startsWith("9")) {
				return 1 % arg2;
				// if the string starts with n-z or anything else... n is another very popular
				// letter...so we are splitting all three up
			} else {
				return 2 % arg2;
			}

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
		Job job = Job.getInstance(conf, "Pairs Partitioner Approach");
		job.setJarByClass(WordPairMapReducewithPartitioner.class);
		job.setMapperClass(WordPairMapper.class);
		job.setPartitionerClass(WordPairPartitioner.class);
		//job.setCombinerClass(WordPairReducer.class);
		job.setReducerClass(WordPairReducer.class);
		job.setNumReduceTasks(3);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
