package edu.uta.cse6331;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Aditya on October 03, 2017.
 * 1001429814
 */

public class Multiply {

    static class Mapper1Input1 extends Mapper<Object, Text, IntWritable, MatrixElement> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int i, j;
            double v;
            i = Integer.parseInt(line.split(",")[0]);
            j = Integer.parseInt(line.split(",")[1]);
            v = Double.parseDouble(line.split(",")[2]);
            context.write(new IntWritable(j), new MatrixElement((short) 0, i, v));
        }
    }

    static class Mapper1Input2 extends Mapper<Object, Text, IntWritable, MatrixElement> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int i, j;
            double v;
            i = Integer.parseInt(line.split(",")[0]);
            j = Integer.parseInt(line.split(",")[1]);
            v = Double.parseDouble(line.split(",")[2]);
            context.write(new IntWritable(j), new MatrixElement((short) 1, i, v));
        }
    }

    static class Reducer1BothReducers extends Reducer<IntWritable, MatrixElement, Pair, DoubleWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<MatrixElement> values, Context context) throws IOException, InterruptedException {
            ArrayList<MatrixElement> matAElements = new ArrayList<>();
            ArrayList<MatrixElement> matBElements = new ArrayList<>();
            values.forEach(value -> {
                if (value.tag == 0) matAElements.add(new MatrixElement(value.tag, value.index, value.value));
                if (value.tag == 1) matBElements.add(new MatrixElement(value.tag, value.index, value.value));
            });
            matAElements.forEach(matAElement -> {
                matBElements.forEach(matBElement -> {
                    try {
                        context.write(new Pair(matAElement.index, matBElement.index), new DoubleWritable(matAElement.value * matBElement.value));
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
            });

        }
    }

    static class Mapper2 extends Mapper<Object, Object, Pair, DoubleWritable> {
        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String key1 = line.split("\\t")[0];
            String value1 = line.split("\\t")[1];
            key1 = key1.replace("(", "");
            key1 = key1.replace(")", "");


            int i, j;
            i = Integer.parseInt(key1.split(",")[0]);
            j = Integer.parseInt(key1.split(",")[1]);
            Pair pair = new Pair(i, j);

            double valuee = Double.parseDouble(value1);
            context.write(pair, new DoubleWritable(valuee));
        }
    }

    static class Reducer2 extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {
        @Override
        protected void reduce(Pair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }
            context.write(new Pair(key.i, key.j), new DoubleWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception {
        Job firstPhase = Job.getInstance();
        firstPhase.setJobName("FIRST PHASE");
        firstPhase.setJarByClass(Multiply.class);
        firstPhase.setOutputKeyClass(Pair.class);
        firstPhase.setOutputValueClass(DoubleWritable.class);
        firstPhase.setMapOutputKeyClass(IntWritable.class);
        firstPhase.setMapOutputValueClass(MatrixElement.class);
        firstPhase.setReducerClass(Reducer1BothReducers.class);
        firstPhase.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(
                firstPhase,
                new Path(args[0]),
                TextInputFormat.class,
                Mapper1Input1.class
        );
        MultipleInputs.addInputPath(
                firstPhase,
                new Path(args[1]),
                TextInputFormat.class,
                Mapper1Input2.class
        );
        FileOutputFormat.setOutputPath(firstPhase, new Path(args[2]));
        firstPhase.waitForCompletion(true);

        Job secondPhase = Job.getInstance();
        secondPhase.setJobName("SECOND PHASE");
        secondPhase.setJarByClass(Multiply.class);
        secondPhase.setOutputKeyClass(Pair.class);
        secondPhase.setOutputValueClass(DoubleWritable.class);
        secondPhase.setMapOutputKeyClass(Pair.class);
        secondPhase.setMapOutputValueClass(DoubleWritable.class);
        secondPhase.setMapperClass(Mapper2.class);
        secondPhase.setReducerClass(Reducer2.class);
        secondPhase.setInputFormatClass(TextInputFormat.class);
        secondPhase.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(secondPhase, new Path(args[2]));
        FileOutputFormat.setOutputPath(secondPhase, new Path(args[3]));
        secondPhase.waitForCompletion(true);
    }

    static class Pair implements WritableComparable<Pair> {
        int i;
        int j;

        Pair() {
        }

        public Pair(int i, int j) {
            this.i = i;
            this.j = j;
        }

        @Override
        public int compareTo(Pair b) {
            if (b.i == this.i && b.j == this.j) return 0;

            if (b.i > this.i) {
                return 1;
            }

            if (b.i < this.i) {
                return -1;
            }

            if (b.i == this.i) {
                if (b.j > this.j)
                    return 1;
                if (b.j < this.j)
                    return -1;
            }
            return -1;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(i);
            dataOutput.writeInt(j);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            i = dataInput.readInt();
            j = dataInput.readInt();
        }

        @Override
        public String toString() {
            return "(" + this.i + "," + this.j + ")";
        }
    }

    static class MatrixElement implements Writable {

        MatrixElement() {
        }

        short tag;
        int index;
        double value;

        public MatrixElement(short tag, int index, double value) {
            this.tag = tag;
            this.index = index;
            this.value = value;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeShort(tag);
            dataOutput.writeInt(index);
            dataOutput.writeDouble(value);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            tag = dataInput.readShort();
            index = dataInput.readInt();
            value = dataInput.readDouble();
        }
    }
}

