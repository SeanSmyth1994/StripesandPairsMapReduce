package StripesCombiner;

import java.io.IOException;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* Map
 * key = word
 * value = map of neighbours and their partial sum
 * Reduce
 * key = word
 * value = neighbours and their elemental sums
 * code inspired by http://codingjunkie.net/cooccurrence/
 * changed the input into the map
 * changed the output so it uses words based on latin characters,
 */
public class StripesMapReducewithCombiner {
	public static class StripesMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
		private Text word = new Text();
		private MapWritable stripes = new MapWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int neighbours = context.getConfiguration().getInt("neighbours", 2);
			String[] words = value.toString().split("\\s+");
			if (words.length > 1) {
				for (int i = 0; i < words.length; i++) {
					String newWord = nopunct(words[i]);
					if (!newWord.contains(" ")) {
						word.set(newWord);
						stripes.clear();
						// basically this gets the neighbours surrounding the word and puts them into a
						// loop
						int start = (i - neighbours < 0) ? 0 : i - neighbours;
						int end = (i + neighbours >= words.length) ? words.length - 1 : i + neighbours;
						for (int j = start; j <= end; j++) {
							if (j == i) {
								continue;
							}
							String neigh = nopunct(words[j]);
							Text neighbour = new Text(neigh);
							if (stripes.containsKey(neighbour)) {
								// partial sum if neighbour is repeated
								IntWritable count = (IntWritable) stripes.get(neighbour);
								count.set(count.get() + 1);

							} else {
								stripes.put(neighbour, new IntWritable(1));
							}
						}
						context.write(word, stripes);
					}

				}
			}

		}

		public static String nopunct(String word) {
			Pattern pattern = Pattern.compile("[^0-9 a-z A-Z \\\\x00-\\\\x7F]");
			Matcher matcher = pattern.matcher(word);
			String newWord = matcher.replaceAll(" ");
			return newWord;
		}
	}

	public static class StripesReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
		private MapWritable incMap = new MapWritable();

		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {
			incMap.clear();
			for (MapWritable value : values) {
				addAll(value);
			}
			context.write(key, incMap);
		}

		private void addAll(MapWritable mapWrite) {
			Set<Writable> keys = mapWrite.keySet();
			for (Writable key : keys) {
				IntWritable fromCount = (IntWritable) mapWrite.get(key);
				if (incMap.containsKey(key)) {
					IntWritable count = (IntWritable) incMap.get(key);
					count.set(count.get() + fromCount.get());
				} else {
					incMap.put(key, fromCount);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Stripes With Combiner Approach");
		job.setJarByClass(StripesMapReducewithCombiner.class);
		job.setMapperClass(StripesMapper.class);
		// setting combiner class to reducer class
		job.setCombinerClass(StripesReducer.class);
		job.setReducerClass(StripesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
