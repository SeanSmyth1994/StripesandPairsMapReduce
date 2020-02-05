package WordPairInMap;

import java.io.DataInput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordPairMapReduceInMap {
	public static class WordPairMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Map<String, Integer> map;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = clean(value.toString());
			String secondword = new String();
			String firstword = new String();
			// associative array
			Map<String, Integer> map = getMap();
			StringTokenizer itr = new StringTokenizer(line);

			if (itr.hasMoreTokens()) {
				firstword = itr.nextToken();
				secondword = firstword;
			}
			while (itr.hasMoreTokens()) {
				firstword = secondword;
				secondword = itr.nextToken();

				firstword = firstword + " " + secondword;
				// combine
				if (map.containsKey(firstword.toLowerCase())) {
					int total = map.get(firstword.toLowerCase()) + 1;
					map.put(firstword.toLowerCase(), total);
				} else {
					map.put(firstword.toLowerCase(), 1);
				}
				// At the end of each line it will only take the last word as key.
				if (!itr.hasMoreTokens()) {

					map.put(secondword.toLowerCase(), 1);
				}
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			Map<String, Integer> map = getMap();
			Iterator<Map.Entry<String, Integer>> it = map.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, Integer> entry = it.next();
				String sKey = entry.getKey();
				int total = entry.getValue().intValue();
				context.write(new Text(sKey), new IntWritable(total));
			}
		}

		public Map<String, Integer> getMap() {
			if (null == map) // lazy loading
				map = new HashMap<String, Integer>();
			return map;
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
		Job job = Job.getInstance(conf, "Pairs In Mapper Approach");
		job.setJarByClass(WordPairMapReduceInMap.class);
		job.setMapperClass(WordPairMapper.class);
		job.setReducerClass(WordPairReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
