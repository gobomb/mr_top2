package kaola;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TopSort extends Configured implements Tool {
    public static class DateOccurrenceKey implements WritableComparable<DateOccurrenceKey> {

        protected String keyDate = new String();
        protected Integer keyOccurrence;

        public String getKeyDate() {
            return keyDate;
        }

        public void setKeyDate(String date) {
            this.keyDate = date;
        }

        public Integer getKeyOccurrence() {
            return keyOccurrence;
        }

        public void setKeyOccurrence(Integer keyOccurrence) {
            this.keyOccurrence = keyOccurrence;
        }

        DateOccurrenceKey(String date, Integer keyOccurrence) {
            this.keyDate = date;
            this.keyOccurrence = keyOccurrence;
        }

        DateOccurrenceKey() {
        }

        @Override
        public void write(DataOutput d) throws IOException {
            d.writeUTF(keyDate);
            d.writeInt(keyOccurrence);

        }

        @Override
        public void readFields(DataInput di) throws IOException {
            keyDate = di.readUTF();
            keyOccurrence = di.readInt();
        }

        @Override
        public String toString() {
            return keyDate.concat("   ".concat(keyOccurrence.toString()));
        }

        @Override
        public int compareTo(DateOccurrenceKey t) {
            int dateDiff = keyDate.compareTo(t.keyDate);
            if (dateDiff != 0) {
                return -1 * dateDiff;
            }

            return -1 * keyOccurrence.compareTo(t.keyOccurrence);
        }
    }

    public static class OccurrenceMapper extends Mapper<Object, Text, DateOccurrenceKey, Text> {

        private final Logger logger = Logger.getLogger(OccurrenceMapper.class);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\s+");
            if (tokens.length < 3) {
                logger.warn("malformed record ".concat(value.toString()));
                return;
            }

            logger.info("Token ".concat(tokens.toString()));

            DateOccurrenceKey dateKey = new DateOccurrenceKey(tokens[0], Integer.parseInt(tokens[2]));
            context.write(dateKey, new Text(tokens[1]));
        }
    }

    public static class DatePartitioner extends Partitioner<DateOccurrenceKey, Text> {

        @Override
        public int getPartition(DateOccurrenceKey key, Text value, int i) {
            return key.getKeyDate().hashCode() % i;
        }

    }

    public static class DateGroupComparator extends WritableComparator {

        protected DateGroupComparator() {
            super(DateOccurrenceKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            DateOccurrenceKey keyA = (DateOccurrenceKey) a;
            DateOccurrenceKey keyB = (DateOccurrenceKey) b;
            return keyA.getKeyDate().compareTo(keyB.getKeyDate());
        }
    }

    public static class KeyAttributeReducer extends Reducer<DateOccurrenceKey, Text, Text, Text> {

        private final Logger logger = Logger.getLogger(TopCounter.KeyAttributeMapper.class);

        public void reduce(DateOccurrenceKey key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            logger.info("Top N " + Config.topN);
            String val_cum = "";
            int i = 0;
            for (Text val : values) {
                if (i >= Config.topN) {
                    break;
                }

                i++;
                if (!val_cum.isEmpty()) {
                    val_cum += ",";
                }
                val_cum += val.toString();
            }

            context.write(new Text(key.keyDate), new Text(val_cum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TopSort");
        job.setJarByClass(getClass());
        job.setMapperClass(OccurrenceMapper.class);
        job.setReducerClass(KeyAttributeReducer.class);
        job.setPartitionerClass(DatePartitioner.class);
        job.setGroupingComparatorClass(DateGroupComparator.class);

        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(DateOccurrenceKey.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileSystem filesystem = FileSystem.get(conf);
        filesystem.delete(new Path(args[1]), true);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
