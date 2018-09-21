package topten;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class TopTen_alt {
	// This helper function parses the stackoverflow into a Map for us.
	public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
			for (int i = 0; i < tokens.length - 1; i += 2) {
				String key = tokens[i].trim();
				String val = tokens[i + 1];
				map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xml);
		}

		return map;
	}

	public static class TopTenMapper extends Mapper<Object, org.apache.hadoop.io.Text, NullWritable, org.apache.hadoop.io.Text> {
		// Stores a map of user reputation to the record
		TreeMap<Integer, org.apache.hadoop.io.Text> repToRecordMap = new TreeMap<Integer, org.apache.hadoop.io.Text>();

		public void map(Object key, org.apache.hadoop.io.Text value, Context context) throws IOException, InterruptedException {
			Map<String, String> map = transformXmlToMap(value.toString());
			if (map.containsKey("Id") && map.containsKey("Reputation")) {
				int K = Integer.parseInt(map.get("Id")); // Id is Key
				org.apache.hadoop.io.Text V = new org.apache.hadoop.io.Text(map.get("Reputation")); // Reputation is Value
				repToRecordMap.put(K, V);
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Output our ten records to the reducers with a null key
			org.apache.hadoop.io.Text myText = new org.apache.hadoop.io.Text();
			for (int i = 0; i < 10; i++) {
				Entry<Integer, org.apache.hadoop.io.Text> entry = repToRecordMap.pollLastEntry();
				// Writes "[int];[text]" to Context
				myText.set(entry.getKey().toString() + ";" + entry.getValue());
				context.write(NullWritable.get(), myText);
			}
		}
	}

	public static class TopTenReducer extends TableReducer<NullWritable, org.apache.hadoop.io.Text, NullWritable> {
		// Stores a map of user reputation to the record
		private TreeMap<Integer, org.apache.hadoop.io.Text> repToRecordMap = new TreeMap<Integer, org.apache.hadoop.io.Text>();

		public void reduce(NullWritable key, Iterable<org.apache.hadoop.io.Text> values, Context context)
				throws IOException, InterruptedException {
			// Place all records into TreeMap
			for (org.apache.hadoop.io.Text t : values) {
				String s = t.toString();
				// Reads the line up to ";" and stores that as Key and the full original Text as
				// value
				repToRecordMap.put(Integer.parseInt(s.substring(0, s.indexOf(";"))), t);
			}
			// Extract Top 10 records and write to result
			for (int i = 0; i < 10; i++) {
				try {
					Entry<Integer, org.apache.hadoop.io.Text> entry = repToRecordMap.pollLastEntry();
					/*
					Put insHBase = new Put(entry.getValue().getBytes());
					insHBase.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sum"), Bytes.toBytes(sum));
					context.write(NullWritable.get(), insHBase);
					 */
					// create hbase put with rowkey as date
					Put insHBase = new Put(entry.getValue().getBytes());

					// insert sum value to hbase 
					insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("data"), entry.getValue().getBytes());

					// write data to Hbase table
					context.write(null, insHBase);
				}catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "top ten");
		job.setJarByClass(TopTen.class);

		job.setMapperClass(TopTenMapper.class);
		// job.setCombinerClass(TopTenReducer.class);
		job.setReducerClass(TopTenReducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(org.apache.hadoop.io.NullWritable.class);
		job.setOutputValueClass(org.apache.hadoop.io.Text.class);

		job.setMapOutputKeyClass(org.apache.hadoop.io.NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// define scan and define column families to scan
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("info"));

		// define input hbase table
		TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);

		job.waitForCompletion(true);
	}
}
