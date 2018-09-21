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
// import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TopTen {
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
			// System.err.println(xml);
		}

		return map;
	}

	public static class TopTenMapper extends Mapper<Object, Text, org.apache.hadoop.io.NullWritable, Text> {
		// Stores a map of user reputation to the record
		TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		private static final Log LOG = LogFactory.getLog(TopTen.class);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {			
			Map<String, String> map = transformXmlToMap(value.toString());
			
			if (!map.isEmpty()) {
				String s_userRep = map.get("Reputation");
				String s_userId = map.get("Id");
	
				if (map.containsKey("Id") && map.containsKey("Reputation")) {
	
					Integer userRep = Integer.parseInt(s_userRep);
					repToRecordMap.put(userRep, value);

					context.write(NullWritable.get(), value);
				}
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Output our ten records to the reducers with a null key
			int count = 0;

			for (Map.Entry<Integer, Text> entry: repToRecordMap.descendingMap().entrySet()) {
				if(count >= 10) break;

				context.write(NullWritable.get(), entry.getValue());
				count++;
			}
		}
	}

	public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
		// Stores a map of user reputation to the record
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		private static final Log LOG = LogFactory.getLog(TopTen.class);

		public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			for (Text t : values) {
				String s = t.toString();
				Map<String, String> userRow = transformXmlToMap(s);
				
				if (!userRow.isEmpty()) {
					if (userRow.containsKey("Id") && userRow.containsKey("Reputation")) {
						
						Integer userRep = Integer.parseInt(userRow.get("Reputation"));
						Text userId = new Text(userRow.get("Id"));
						
						repToRecordMap.put(userRep, userId);
					}
				}

			}
			// Extract Top 10 records and write to result
			int count = 0;
			for (Map.Entry<Integer, Text> entry : repToRecordMap.descendingMap().entrySet()) {
				if(count >= 10) break;

				Integer userRep = entry.getKey();
				Text userId = new Text(entry.getValue());

				Put insHBase = new Put(Bytes.toBytes(count));
				insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes(userRep.toString()));
				insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(userId.toString()));

				context.write(NullWritable.get(), insHBase);
				count++;
			}

			// context.write(NullWritable.get());
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
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
