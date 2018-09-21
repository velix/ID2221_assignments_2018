package topten;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

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
				System.err.println(xml);
			}

			return map;
		}

		public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
			// Stores a map of user reputation to the record
			TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				// The value parameter is probably a single row from the xml file
				Map<String, String> user = transformXmlToMap(value.toString());

				String userId = new Text(user.get("Id"));
				int userRep = Integer.parseInt(user.get("Reputation"));

				// Validity checks
				if(userId != null & Integer.parseInt(userID) >= 0){
					// repToRecordMap.put(userRep, userId);
					repToRecordMap.put(userRep, value);
				}

			}

			protected void cleanup(Context context) throws IOException, InterruptedException {
				// Output our ten records to the reducers with a null key
				int count = 0;

				// For each of the top 10 entries in the TreeMap,
				// emmit its value (which is the whole user row)
				for (Map.Entry<Integer, Text> entry : repToRecordMap.descendingMap().entrySet()) {
					if (count > 10) break;

					context.write(NullWritable.get(), entry.getValue());
					count++;
				}

				
			}
		}

		public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
			// Stores a map of user reputation to the record
			private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

			public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

				// Since the Mapper emmited the whole row for each entry,
				// the values iterable contains the whole user rows
				for (Text user_row : values) {
					Map<String, String> user = transformXmlToMap(user_row);

					Text userId = new Text(user.get("Id"));
					int userRep = Integer.parseInt(user.get("Reputation"));
				
					repToRecordMap.put(userRep, userId);
				}

				int count = 0;

				for (Map.Entry<Integer, Text> entry : repToRecordMap.descendingMap().entrySet()) {
					if (count > 10) break;

					// create hbase put with rowkey
					Put insHBase = new Put(entry.getKey().getBytes());

					// insert rep and id values to HBase
					insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes(entry.getKey()));
					insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(entry.getValue()));

					// write data to Hbase table
					context.write(null, insHBase);
					count++;
				}


			}
		}

		public static void main(String[] args) throws Exception {
			// <FILL IN>
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "top ten");
			job.setJarByClass(TopTen.class);
			
			job.setMapperClass(TopTenMapper.class);
			job.setCombinerClass(TopTenReducer.class);
			job.setReducerClass(TopTenReducer.class);
			job.setNumReduceTasks(1);

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
