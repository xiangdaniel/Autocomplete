import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
	MapReduce Job1: build N-Gram (mapper) and sum total counts (reducer)
 */
public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;
		@Override
		public void setup(Context context) {
			//get n-gram from command line
			Configuration configuration = context.getConfiguration();
			noGram = configuration.getInt("noGram", 5);
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			
			line = line.trim().toLowerCase();

			//remove useless elements
			line = line.replaceAll("[^a-z]", " ");

			//separate word by space
			String[] words = line.split("\\s+"); // split by ' ', '\t' etc

			if (words.length < 2) {
				return;
			}

			//build n-gram based on array of words
			StringBuilder ngram;
			for (int i = 0; i < words.length - 1; i++) {
				ngram = new StringBuilder();
				ngram.append(words[i]);
				for (int j = i + 1; j < words.length && j - i < noGram; j++) {
					ngram.append(" ");
					ngram.append(words[j]);
					context.write(new Text(ngram.toString().trim()), new IntWritable(1));
				}
			}
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			//how to sum up the total count for each n-gram?
			int count = 0;
			for (IntWritable value: values) {
				count += value.get();
			}

			context.write(key, new IntWritable(count));
		}
	}

}
