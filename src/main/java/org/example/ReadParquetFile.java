package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.io.IOException;

public class ReadParquetFile {

    //private static Path parquetFilepath = new Path("file:\\D:\\Parquet/userdata1.parquet");

   private static File file = new File("D:\\Parquet/userdata1.parquet");


    public static void main(String [] args) throws IOException {
        ReadParquetFile obj = new ReadParquetFile();
        System.out.println(new File(".").getAbsoluteFile());
        System.out.println(file);
        System.out.println(file.exists());
        System.out.println(file.canRead());
        obj.readParquet(file);

    }

    private void readParquet(final File f) throws IOException
    {
        final Path path = new Path(f.toURI());
        final HadoopInputFile hif = HadoopInputFile.fromPath(path, new Configuration());
        final ParquetReadOptions opts = HadoopReadOptions.builder(hif.getConfiguration())
                .withMetadataFilter(ParquetMetadataConverter.NO_FILTER).build();

        try (final ParquetFileReader r = ParquetFileReader.open(hif, opts))
        {
            final ParquetMetadata readFooter = r.getFooter();
            final MessageType schema = readFooter.getFileMetaData().getSchema();

            while (true)
            {
                final PageReadStore pages = r.readNextRowGroup();

                if (pages == null)
                    return;

                final long rows = pages.getRowCount();

                System.out.println("Number of rows: " + rows);

                final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                final RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                for (int i = 0; i < rows; i++)
                {
                    final Group g = (Group) recordReader.read();

                    printGroup(g);
                }
            }
        }
    }

    private static void printGroup(Group g)
    {
        final int fieldCount = g.getType().getFieldCount();

        for (int field = 0; field < fieldCount; field++)
        {
            final int valueCount = g.getFieldRepetitionCount(field);
            final Type fieldType = g.getType().getType(field);
            final String fieldName = fieldType.getName();

            for (int index = 0; index < valueCount; index++)
            {
                if (fieldType.isPrimitive())
                {
                    System.out.println(fieldName + " " + g.getValueToString(field, index));
                }
            }
        }

        System.out.println("");
    }
}
