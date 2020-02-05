package StripesInMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StripesMapReducewithInMap {
	public static class StripesMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
		MapWritable tempMap;
		Map<String, Integer> associativeMap;
		Map<String, Map<String, Integer>> fullMap;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int neighbours = context.getConfiguration().getInt("neighbours", 2);
			String[] words = value.toString().split("\\s+");
			fullMap = getMap();

			if (words.length > 1) {
				for (int i = 0; i < words.length; i++) {
					// basically flushing the map
					// this is just to get rid of the heap and timeout exceptions
					// in practice i could change the heap size of hadoop or have multiple inputs etc.
					// if the full associative array has over 500 records, output the files to disk and flush the map
					// might affect times at a certain point
					if (fullMap.size() > 500) {
						Map<String, Map<String, Integer>> map = getMap();
						System.out.println("number of iterations ==> " + i);
						Iterator<Entry<String, Map<String, Integer>>> it = map.entrySet().iterator();
						while (it.hasNext()) {
							Map.Entry<String, Map<String, Integer>> entry = it.next();
							String sKey = entry.getKey();
							tempMap = new MapWritable();
							Iterator<Entry<String, Integer>> mapped = entry.getValue().entrySet().iterator();
							while (mapped.hasNext()) {
								String n = mapped.next().getKey();
								int fromCount = entry.getValue().get(n);
								Text neigh = new Text(n);
								if (tempMap.containsKey(neigh)) {
									int count = ((IntWritable) tempMap.get(neigh)).get();
									int total = fromCount + count;
									tempMap.put(new Text(n), new IntWritable(total));
								} else {
									tempMap.put(new Text(n), new IntWritable(fromCount));
								}
							}

							context.write(new Text(sKey), tempMap);

						}
						associativeMap.clear();
						tempMap.clear();
						fullMap.clear();
					}
					if (!nopunct(words[i]).contains(" ")) {

						associativeMap = new HashMap<String, Integer>();
						// if we find the word has been a part of a previous map, we will combine them
						// together
						if (fullMap.containsKey(nopunct(words[i]))) {
							// the fullmap has found the key and is able to assign its contents to the
							// associative array
							// if this is triggered it will combine too associative arrays together further
							// down (therefore
							// it will in map combine)
							associativeMap = fullMap.get(nopunct(words[i]));
						}

						int start = (i - neighbours < 0) ? 0 : i - neighbours;
						int end = (i + neighbours >= words.length) ? words.length - 1 : i + neighbours;
						for (int j = start; j <= end; j++) {
							if (j == i) {
								continue;
							}
							Text neighbour = new Text(nopunct(words[j]));
							if (associativeMap.containsKey(neighbour.toString())) {
								int count = associativeMap.get(neighbour.toString()) + 1;
								associativeMap.put(neighbour.toString(), count);

							} else {
								associativeMap.put(neighbour.toString(), 1);

							}
						}
						// fullMap should hold all the stripes
						fullMap.put(nopunct(words[i]), associativeMap);

					}
				}

			}
		}

		public static String nopunct(String word) {
			Pattern pattern = Pattern.compile("[^0-9 a-z A-Z \\x00-\\x7F]");
			Matcher matcher = pattern.matcher(word);
			String newWord = matcher.replaceAll(" ");
			return newWord;
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			Map<String, Map<String, Integer>> map = getMap();

			Iterator<Entry<String, Map<String, Integer>>> it = map.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, Map<String, Integer>> entry = it.next();
				String sKey = entry.getKey();
				tempMap = new MapWritable();
				Iterator<Entry<String, Integer>> mapped = entry.getValue().entrySet().iterator();
				while (mapped.hasNext()) {
					String n = mapped.next().getKey();
					int fromCount = entry.getValue().get(n);
					Text neigh = new Text(n);
					if (tempMap.containsKey(neigh)) {
						int count = ((IntWritable) tempMap.get(neigh)).get();
						int total = fromCount + count;
						tempMap.put(new Text(n), new IntWritable(total));
					} else {
						tempMap.put(new Text(n), new IntWritable(fromCount));
					}
				}

				context.write(new Text(sKey), tempMap);

			}

		}

		public Map<String, Map<String, Integer>> getMap() {
			if (null == fullMap) // lazy loading
				fullMap = new HashMap<String, Map<String, Integer>>();
			return fullMap;
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
		Job job = Job.getInstance(conf, "Stripes With In Mapper Combining");
		job.setJarByClass(StripesMapReducewithInMap.class);
		job.setMapperClass(StripesMapper.class);
		// job.setCombinerClass(StripesReducer.class);
		// might need to artificially create mappers so it isnt flooding one map.....
		job.setReducerClass(StripesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
