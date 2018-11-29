package kaola;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TopCounter extends Configured implements Tool {

    public static class DateAttributeKey implements WritableComparable<DateAttributeKey> {

        protected String keyDate = new String();
        protected String keyAttribute = new String();

        public String getKeyDate() {
            return keyDate;
        }

        public void setKeyDate(String date) {
            this.keyDate = date;
        }

        public String getKeyAttribute() {
            return keyAttribute;
        }

        public void setKeyAttribute(String keyAttribute) {
            this.keyAttribute = keyAttribute;
        }

        DateAttributeKey(String date, String attribute) {
            this.keyDate = date;
            this.keyAttribute = attribute;
        }

        DateAttributeKey() {
        }

        @Override
        public void write(DataOutput d) throws IOException {
            d.writeUTF(keyDate);
            d.writeUTF(keyAttribute);

        }

        @Override
        public void readFields(DataInput di) throws IOException {
            keyDate = di.readUTF();
            keyAttribute = di.readUTF();
        }

        @Override
        public String toString() {
            return keyDate.concat("   ".concat(keyAttribute));
        }

        @Override
        public int compareTo(DateAttributeKey t) {
            int dateDiff = keyDate.compareTo(t.keyDate);
            if (dateDiff != 0) {
                return dateDiff;
            }

            return keyAttribute.compareTo(t.keyAttribute);
        }
    }

    public static class KeyAttributeMapper extends Mapper<Object, Text, DateAttributeKey, IntWritable> {

        private final Logger logger = Logger.getLogger(KeyAttributeMapper.class);
        private final static IntWritable one = new IntWritable(1);
        private final static int dateIdx = 8;
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            logger.info("keyAttributeIdx is " + Config.keyAttributeIdx);
            String[] tokens = value.toString().split("\\|");
            if (tokens.length <= dateIdx || tokens.length <= Config.keyAttributeIdx) {
                logger.warn("malformed record ".concat(value.toString()));
                return;
            }

            String date = tokens[dateIdx].split("\\s+")[0];
            if (date.isEmpty()) {
                logger.warn("malformed date ".concat(date));
                return;
            }

            logger.info("Token size " + tokens.length);
            logger.info("Date " + date);
            logger.info("KeyAttribute Text ".concat(tokens[Config.keyAttributeIdx]));

            String[] keyAttrs = tokens[Config.keyAttributeIdx].split(",");
            logger.info("Found " + keyAttrs.length + " Key Attributes");

            for (String attr : keyAttrs) {
                DateAttributeKey dateKey = new DateAttributeKey(date, attr);
                word.set(dateKey.toString());
                context.write(dateKey, one);
            }
        }
    }

    public static class DatePartitioner extends Partitioner<DateAttributeKey, Text> {

        @Override
        public int getPartition(DateAttributeKey key, Text value, int i) {
            return key.getKeyDate().hashCode() % i;
        }

    }

    public static class DateGroupComparator extends WritableComparator {

        protected DateGroupComparator() {
            super(DateAttributeKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            DateAttributeKey keyA = (DateAttributeKey) a;
            DateAttributeKey keyB = (DateAttributeKey) b;
            return keyA.getKeyDate().compareTo(keyB.getKeyDate());
        }
    }

    public static class KeyAttributeReducer extends Reducer<DateAttributeKey, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(DateAttributeKey key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(new Text(key.toString()), result);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TopCounter");
        job.setJarByClass(getClass());
        job.setMapperClass(KeyAttributeMapper.class);
        job.setReducerClass(KeyAttributeReducer.class);

        job.setOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(DateAttributeKey.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileSystem filesystem = FileSystem.get(conf);
        filesystem.delete(new Path(args[1]), true);
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
