import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CSVLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CSVNLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by markmo on 7/05/2016.
 */
public class App extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    protected static enum LineCounter {
        NUMBER_LINES,
        RECORDS_WITH_EMPTY_PROPS,
        INVALID_COLUMN_COUNT,
        INVALID_CUSTOMER_ID,
        INVALID_EVENT_TYPE,
        INVALID_TS
    }

    public static class MapTask extends Mapper<LongWritable, List<Text>, NullWritable, Text> {

        private static final String quote = "\"";
        private static final String delim = ",";
        private static final String UNKNOWN = "UNKNOWN";

        private Text text = new Text();
        private String[] strings = new String[11];

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        /**
         * values:
         * 0  customer_id_type_id
         * 1  customer_id
         * 2  event_type_id
         * 3  event_ts
         * 4  event_version
         * 5  event_property
         * 6  source_key
         * 7  job_id
         * 8  process_name
         * 9  created_ts
         * 10 value
         */
        public void map(LongWritable key, List<Text> values, Context context) throws IOException, InterruptedException {
            // check valid number of columns
            if (values.size() != 11) {
                context.getCounter(LineCounter.INVALID_COLUMN_COUNT).increment(1);
                logger.warn("Invalid record (" + values.size() + " columns): " + values);
                return;
            }

            for (int i = 0; i < 11; i++) {
                strings[i] = values.get(i).toString();
            }

            // let invalid customer ids through
            // they might be resolved later using a session id

            // check valid customer id
            String customerIdTypeId = strings[0];
            if (customerIdTypeId == null || customerIdTypeId.isEmpty()) {
                context.getCounter(LineCounter.INVALID_CUSTOMER_ID).increment(1);
                //return;
            } else {
                try {
                    int typeId = Integer.parseInt(customerIdTypeId);
                    if (typeId < 0) {
                        context.getCounter(LineCounter.INVALID_CUSTOMER_ID).increment(1);
                        //return;
                    }
                } catch (NumberFormatException e) {
                    context.getCounter(LineCounter.INVALID_CUSTOMER_ID).increment(1);
                    //return;
                }
            }
            customerIdTypeId = null;

            String customerId = strings[1];
            if (customerId == null || customerId.isEmpty() || UNKNOWN.equals(customerId)) {
                context.getCounter(LineCounter.INVALID_CUSTOMER_ID).increment(1);
                //return;
            }
            customerId = null;

            // check valid event type
            String eventTypeId = strings[2];
            if (eventTypeId == null || eventTypeId.isEmpty() || UNKNOWN.equals(eventTypeId)) {
                context.getCounter(LineCounter.INVALID_EVENT_TYPE).increment(1);
                return;
            }
            try {
                int typeId = Integer.parseInt(eventTypeId);
                if (typeId < 0) {
                    context.getCounter(LineCounter.INVALID_EVENT_TYPE).increment(1);
                    return;
                }
            } catch (NumberFormatException e) {
                context.getCounter(LineCounter.INVALID_EVENT_TYPE).increment(1);
                return;
            }
            eventTypeId = null;

            // check valid time
            String ts = strings[3];
            if (ts == null || ts.isEmpty()) {
                context.getCounter(LineCounter.INVALID_TS).increment(1);
                return;
            }
            if (ts.startsWith("1970")) {
                context.getCounter(LineCounter.INVALID_TS).increment(1);
                return;
            }
            ts = null;

            // clean props of newline characters
            String props = strings[5];
            if (props != null && !props.isEmpty()) {
                String clean = props.replaceAll("\\\\n", "\\\\\\\\n");
                strings[5] = quoted(clean);
                clean = null;
            } else {
                context.getCounter(LineCounter.RECORDS_WITH_EMPTY_PROPS).increment(1);
            }
            props = null;

            // source key is not used
            strings[6] = "";

            String value = strings[10];
            if (value != null && !value.isEmpty()) {
                strings[10] = quoted(value);
            }
            value = null;

            context.getCounter(LineCounter.NUMBER_LINES).increment(1);

            context.write(NullWritable.get(), join(strings));
        }

        private String quoted(String val) {
            return quote + val + quote;
        }

        private Text join(String[] values) {
            StringBuilder sb = new StringBuilder();
            for (String val : values) {
                sb.append(val).append(delim);
            }
            // reuse text property
            text.set(sb.toString().substring(0, sb.length() - 1));
            sb = null;
            return text;
        }
    }

//    public static class ReduceTask extends Reducer<Text, LongWritable, Text, LongWritable> {
//
//        @Override
//        protected void setup(Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
//            super.setup(context);
//        }
//
//        @Override
//        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
//
//        }
//    }

    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        logger.info("Starting job");

        boolean isZip = "1".equals(args[1]);
        logger.info("isZip: " + isZip);

        String outputCompression = args[2];
        logger.info("output compression: " + outputCompression);

        Configuration conf = new Configuration();
        conf.set(CSVLineRecordReader.FORMAT_DELIMITER, "\"");
        conf.set(CSVLineRecordReader.FORMAT_SEPARATOR, ",");
        conf.setInt(CSVNLineInputFormat.LINES_PER_MAP, 40000);
        conf.setBoolean(CSVLineRecordReader.IS_ZIPFILE, isZip);
        conf.set(CSVLineRecordReader.VALID_LINE_START_PATTERN, "-?\\d+,");
        conf.setInt(CSVLineRecordReader.EXPECTED_COLUMN_COUNT, 11);

//        conf.setBoolean("mapreduce.map.output.compress", true);
//        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
//        conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
//        conf.set("mapreduce.map.java.opts", "-Xms256m -Xmx8g -XX:-UseConcMarkSweepGC -XX:-UseGCOverheadLimit");
//        conf.set("mapreduce.map.memory.mb", "8192");

        String inputPath = args[3];
        String outputPath = args[4];

        logger.info("input path: " + inputPath);
        logger.info("output path: " + outputPath);

        if (args.length == 5) {
            logger.info("simple mode");

            Job job = Job.getInstance(conf, "csvfix");
            job.setJarByClass(App.class);
            job.setNumReduceTasks(0);

            MultithreadedMapper.setMapperClass(job, MapTask.class);
            MultithreadedMapper.setNumberOfThreads(job, 8);
            job.setMapperClass(MultithreadedMapper.class);

//            job.setReducerClass(ReduceTask.class);
//            job.setOutputKeyClass(Text.class);
//            job.setOutputValueClass(LongWritable.class);

            job.setInputFormatClass(CSVNLineInputFormat.class);

            job.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.setInputPaths(job, new Path(inputPath));
            Path outPath = new Path(outputPath);
            FileOutputFormat.setOutputPath(job, outPath);
            FileOutputFormat.setCompressOutput(job, true);

            if ("snappy".equals(outputCompression.toLowerCase())) {
                FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
            } else {
                FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
            }

            outPath.getFileSystem(conf).delete(outPath, true);

            logger.info("running job");

            return job.waitForCompletion(true) ? 0 : 1;

        } else {
            DateTime start = DateTime.parse(args[5]);
            DateTime end = DateTime.parse(args[6]);
            boolean byHour = "hour".equals(args[7]);
            boolean ok = true;
            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMMdd");
            DateTimeFormatter hourFormatter = DateTimeFormat.forPattern("yyyyMMddHH");
            DateTime dt = start;

            while (ok && dt.isBefore(end)) {
                String key;
                Path path;
                if (byHour) {
                    key = hourFormatter.print(dt);
                    path = new Path(inputPath + "/part_time=" + key + "/*");
                } else {
                    key = formatter.print(dt);
                    path = new Path(inputPath + "/part_time=" + key + "*/*");
                }

                logger.info("Cleaning records for key: " + key);

                Job job = Job.getInstance(conf, "csvfix_" + key);
                job.setJarByClass(App.class);
                job.setNumReduceTasks(0);

                MultithreadedMapper.setMapperClass(job, MapTask.class);
                MultithreadedMapper.setNumberOfThreads(job, 8);
                job.setMapperClass(MultithreadedMapper.class);

//                job.setReducerClass(ReduceTask.class);
//                job.setOutputKeyClass(Text.class);
//                job.setOutputValueClass(LongWritable.class);

                job.setInputFormatClass(CSVNLineInputFormat.class);

                job.setOutputFormatClass(TextOutputFormat.class);

                FileInputFormat.setInputPaths(job, path);
                Path outPath = new Path(outputPath + "/part_time=" + key);
                FileOutputFormat.setOutputPath(job, outPath);
                FileOutputFormat.setCompressOutput(job, true);

                if ("snappy".equals(outputCompression.toLowerCase())) {
                    FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
                } else {
                    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
                }

                outPath.getFileSystem(conf).delete(outPath, true);

                logger.info("running job");

                try {
                    ok = job.waitForCompletion(true);

                    logger.info("job done: " + ok);

                } catch (InvalidInputException e) {
                    logger.info("No records on " + key);
                }

                if (byHour) {
                    dt = dt.plusHours(1);
                } else {
                    dt = dt.plusDays(1);
                }
            }
            logger.info("Report for " + formatter.print(start) + " to " + formatter.print(end));

            return ok ? 0 : 1;
        }
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new App(), args);
        System.exit(ret);
    }
}
