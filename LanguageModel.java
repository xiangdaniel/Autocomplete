import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * MapReduce Job2: split N-Gram to starting_phrase, following_phrase=count(Mapper) and write to database(Reducer)
 */
public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threshold;

		@Override
		public void setup(Context context) {
			// get the threshold parameter from the configuration
			Configuration configuration = context.getConfiguration();
			threshold = configuration.getInt("threshold", 20);
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//value example, this is cool\t20
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);

			//filter the n-gram lower than threshold
			if (count < threshold) {
				return;
			}

			//this is --> cool = 20
			//outputkey, this is 
			//outputvalue, cool = 20
			StringBuilder outputKey = new StringBuilder();
			for (int i = 0; i < words.length - 1; i++) {
				outputKey.append(words[i]).append(" ");
			}

			String outputValue = words[words.length - 1] + "=" + count;

			//write key-value to reducer
			context.write(new Text(outputKey.toString().trim()), new Text(outputValue.trim()));
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			//rank top n phrases then write to hdfs
			/* key: i love
			   values: <you=300, her=200, BigData=100>
			 */
			TreeMap<Integer, List<String>> map = new TreeMap<Integer, List<String>>(Collections.reverseOrder());
			for (Text value: values) {
				String phrase = value.toString().trim().split("=")[0];
				int count = Integer.parseInt(value.toString().trim().split("=")[1]);

				if (map.containsKey(count)) {
					map.get(count).add(phrase);
				} else {
					map.put(count, new ArrayList<String>());
					map.get(count).add(phrase);
				}
			}

			Iterator<Integer> iterator = map.keySet().iterator();
			for (int i = 0; iterator.hasNext() && i < n; ) {
				int count = iterator.next();
				List<String> list = map.get(count);
				for (int j = 0; j < list.size(); j++) {
					context.write(new DBOutputWritable(key.toString(), list.get(j), count), NullWritable.get());
					i++;
				}
			}
		}
	}
}
