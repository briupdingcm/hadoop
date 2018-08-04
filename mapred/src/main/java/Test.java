import org.apache.hadoop.io.IntWritable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Test {
    public static void main(String[] args) {
        Iterator<String> sourceIterator = Arrays.asList("A", "B", "C").iterator();
        Stream<String> targetStream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(sourceIterator, Spliterator.ORDERED),
                false);

        targetStream.forEach(System.out::println);

        Iterator<IntWritable> sourceIterator1 = Arrays.asList(new IntWritable(100), new IntWritable(200), new IntWritable(300)).iterator();

        Iterable<IntWritable> iterable = () -> sourceIterator1;
        Stream<IntWritable> stream = StreamSupport.stream(iterable.spliterator(), false);
        int count = stream.reduce(0,
                (c, e) -> {
                    return c + e.get();
                },
                (l, r) -> {
                    return l + r;
                }).intValue();
        System.out.println(count);
    }
}
