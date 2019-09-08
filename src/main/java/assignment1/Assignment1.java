package assignment1;

import java.util.*;

import java.io.IOException;
import java.nio.file.Files;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 
 * This class solves the problem posed for Assignment1
 *
 */
public class Assignment1 {

	// customized writable to record count and src files
	public static class CountAndSrcWritable implements Writable {
		private IntWritable count; // store count
		private Set<String> srcFiles; // store source files

		public CountAndSrcWritable() {
			count = new IntWritable(1); // default count to be 1 for map
			srcFiles = new HashSet<String>(); // initialize empty set
		}
		
		// set 'this' with info in String 
		public void setFromString(String str) { 
			IntWritable _count = new IntWritable();
			Set<String>_srcFiles = new HashSet<String>();
			List<String> strList = Arrays.asList(str.split("\\s+"));
			int count_int = Integer.parseInt(strList.get(0));
			_count.set(count_int);
			for(String file : strList.subList(1, strList.size())) {
				_srcFiles.add(file);
			}
			count = _count;
			srcFiles = _srcFiles;
		}
		
		// define getters and setters (adder)
		public Set<String> getSrcFiles() {
			return srcFiles;
		}

		public int getCount() {
			return count.get();
		}

		public void setCount(int c) {
			count.set(c);
		}

		public void addSrcFiles(String file) {
			srcFiles.add(file);
		}

		// define readFields for Writable
		public void readFields(DataInput in) throws IOException {
			count.readFields(in); // get count first
			Set<String> _srcFiles = new HashSet<String>(); // initialize a buffer
			// get the rest as string and split it with space so it becomes an array
			List<String> files = Arrays.asList(in.readLine().split(" "));
			// loop through the array to put value in buffer
			for (String file : files) {
				_srcFiles.add(file);
			}
			srcFiles = _srcFiles; // set srcFiles to be the buffer
		}

		public void write(DataOutput out) throws IOException {
			count.write(out); // output count first
			out.writeBytes(String.join(" ", srcFiles)); // join value in srcFiles with space and output it
		}

		// override toString function to format final output
		@Override
		public String toString() {
			List<String> fileNames = new ArrayList<String>(srcFiles); // transform set to list
			Collections.sort(fileNames); // sort list
			return count.toString() + "  " + String.join(" ", fileNames); // concatenate and output
		}

	}

	// args[0]: the value N for the ngram
	// args[1]: the minimum count for an ngram to be included in the output
	// args[2]: the directory containing the files in input
	// args[3]: The directory where the output file will be stored.

	public static void main(String[] args) throws Exception {
		int N = Integer.parseInt(args[0]);
		int minCount = Integer.parseInt(args[1]);
		Path middleDir = new Path("/tmp/my_tmp_middle_folder");

		// pass params into configuration
		Configuration conf = new Configuration();
		conf.setInt("N", N);
		conf.setInt("minCount", minCount);
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(middleDir, true); // to get a unused path for the main job to output
		
		// first job is used to do the main works. Mapper extracts info from files. Reducer counts and aggregate the filenames
		Job job1 = Job.getInstance(conf, "main");
		job1.setJarByClass(Assignment1.class);
		job1.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job1.setReducerClass(IntSumReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(CountAndSrcWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[2]));
		FileOutputFormat.setOutputPath(job1, middleDir);
		
		boolean success = job1.waitForCompletion(true);
		if(success) {
			// second job is used to filter unwanted output with Mapper and minCount
			Job job2 = Job.getInstance(conf, "filter");
			job2.setMapperClass(ThresholderMapper.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(CountAndSrcWritable.class);
			FileInputFormat.addInputPath(job2, middleDir);
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
	}
	
	public static class ThresholderMapper extends Mapper<Object, Text, Text, CountAndSrcWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			int minCount = Integer.parseInt(context.getConfiguration().get("minCount"));
			int N = Integer.parseInt(context.getConfiguration().get("N")); // get params from context
			Text newKey = new Text();
			CountAndSrcWritable output = new CountAndSrcWritable();			
			List<String> valueList = Arrays.asList(value.toString().split("\\s+"));
			output.setFromString(String.join(" ", valueList.subList(N, valueList.size())));
			newKey.set(String.join(" ", valueList.subList(0, N)));
			if(output.getCount() >= minCount) {
				context.write(newKey, output); 
			}
		}
	}

	public static class TokenizerMapper extends Mapper<Object, Text, Text, CountAndSrcWritable> {
		private CountAndSrcWritable w; // declare customized writable
		private Text word = new Text(); // initialize Text (writable)

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			w = new CountAndSrcWritable(); // initialize customized writable
			int N = Integer.parseInt(context.getConfiguration().get("N")); // get params from context
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName(); // get filename from context
			w.addSrcFiles(fileName); // put fileName into customized writable

			List<String> words = Arrays.asList(value.toString().trim().split("\\s+")); // get list of word from file
			// loop through list to get ngram
			for (int i = 0; i < words.size() - N + 1; i++) {
				List<String> ngram_list = words.subList(i, i + N);
				String ngram = String.join(" ", ngram_list); // concatenate ngram
				word.set(ngram); // turn ngram into Text (writable)
				context.write(word, w); // output
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, CountAndSrcWritable, Text, CountAndSrcWritable> {
		private CountAndSrcWritable w;

		public void reduce(Text key, Iterable<CountAndSrcWritable> records, Context context)
				throws IOException, InterruptedException {
			// get minCount param from context
			int sum = 0;
			w = new CountAndSrcWritable();
			for (CountAndSrcWritable record : records) {
				sum += record.getCount();
				for (String file : record.getSrcFiles()) {
					w.addSrcFiles(file);
				}
			}			
			w.setCount(sum);
			context.write(key, w);

		}
	}
}