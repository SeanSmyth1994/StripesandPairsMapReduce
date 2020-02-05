package StripesPartitioner;

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
import org.apache.hadoop.mapreduce.Partitioner;
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
 * changed the output so it does words based on latin characters
 */

public class StripesMapReducewithPartitioner {
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

						int start = (i - neighbours < 0) ? 0 : i - neighbours;
						int end = (i + neighbours >= words.length) ? words.length - 1 : i + neighbours;

						for (int j = start; j <= end; j++) {
							if (j == i) {
								continue;
							}
							String neigh = nopunct(words[j]);
							Text neighbour = new Text(neigh);
							if (stripes.containsKey(neighbour)) {
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

	public static class StripesPartitioner extends Partitioner<Text, MapWritable> {

		@Override
		public int getPartition(Text key, MapWritable value, int part) {
			// TODO Auto-generated method stub
			String pair = key.toString();
			if (part == 0) {
				return 0;
			}
			// if we partition correctly we can save reducer time. A and E are the two most
			// popular letters in the alphabet...so lets split them up
			if (pair.startsWith("a") || pair.startsWith("b") || pair.startsWith("c") || pair.startsWith("d")
					|| pair.startsWith("f") || pair.startsWith("g") || pair.startsWith("h") || pair.startsWith("i")
					|| pair.startsWith("j") || pair.startsWith("k") || pair.startsWith("l") || pair.startsWith("m")) {
				return 0;
				// if the string starts with a number
			} else if (pair.startsWith("0") || pair.startsWith("e") || pair.startsWith("1") || pair.startsWith("2")
					|| pair.startsWith("3") || pair.startsWith("4") || pair.startsWith("5") || pair.startsWith("6")
					|| pair.startsWith("7") || pair.startsWith("8") || pair.startsWith("9")) {
				return 1 % part;
				// if the string starts with n-z or anything else
			} else {
				return 2 % part;
			}
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
		Job job = Job.getInstance(conf, "Stripes Partitioner Approach");
		job.setJarByClass(StripesMapReducewithPartitioner.class);
		job.setMapperClass(StripesMapper.class);
		job.setCombinerClass(StripesReducer.class);
		job.setReducerClass(StripesReducer.class);
		job.setNumReduceTasks(3);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
