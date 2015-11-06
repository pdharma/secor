package com.pinterest.secor.io.impl;

import java.io.IOException;
import java.util.Arrays;

import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.FileUtil;
import java.io.File;



/**
 * Sequence file reader writer implementation
 *
 * @author Praveen Murugesan (praveen@uber.com)
 */
public class TextFileReaderWriterFactory implements FileReaderWriterFactory {
    @Override
    public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new TextFileReader(logFilePath);
    }

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
        return new TextFileWriter(logFilePath, codec);
    }

    protected class TextFileReader implements FileReader {
        private java.io.BufferedReader _file;
        private long mOffset;
        

        public TextFileReader(LogFilePath path) throws Exception {
            this._file = new java.io.BufferedReader(new java.io.FileReader(path.getLogFilePath()));
            this.mOffset = path.getOffset();
        }

        @Override
        public KeyValue next() throws IOException {
            String line = _file.readLine();
            if(line != null){
                return new KeyValue(this.mOffset++, line.getBytes("UTF-8"));
            }else{
                return null;
            }

        }

        @Override
        public void close() throws IOException {
            _file.close();
        }
    }

    protected class TextFileWriter implements FileWriter {
        private java.io.BufferedWriter _file;
        private long count = 0;

        public TextFileWriter(LogFilePath path, CompressionCodec codec) throws IOException {
            File ofile = new File(path.getLogFilePath());
            if(!ofile.exists()){
                ofile.getParentFile().mkdirs();
                ofile.createNewFile();
            }
            
            this._file = new java.io.BufferedWriter(new java.io.FileWriter(ofile));
        }

        @Override
        public long getLength() throws IOException {
            return this.count;
        }

        @Override
        public void write(KeyValue keyValue) throws IOException {
            _file.write(System.currentTimeMillis()+"\t"+new String(keyValue.getValue(), "UTF-8"));
            _file.newLine();
            this.count++;
        }

        @Override
        public void close() throws IOException {
            _file.close();
        }
    }
}