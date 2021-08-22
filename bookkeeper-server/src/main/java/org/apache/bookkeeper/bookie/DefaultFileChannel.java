package org.apache.bookkeeper.bookie;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import org.apache.bookkeeper.conf.ServerConfiguration;

class DefaultFileChannel implements BookieFileChannel {
    private final File file;
    private RandomAccessFile randomAccessFile;
    private final ServerConfiguration configuration;

    DefaultFileChannel(File file, ServerConfiguration serverConfiguration) throws IOException {
        this.file = file;
        this.configuration = serverConfiguration;
    }

    @Override
    public FileChannel getFileChannel() throws FileNotFoundException {
        synchronized (this) {
            if (randomAccessFile == null) {
                randomAccessFile = new RandomAccessFile(file, "rw");
            }
            return randomAccessFile.getChannel();
        }
    }

    @Override
    public boolean fileExists(File file) {
        return file.exists();
    }

    @Override
    public FileDescriptor getFD() throws IOException {
        synchronized (this) {
            if (randomAccessFile == null) {
                throw new IOException("randomAccessFile is null, please initialize it by calling getFileChannel");
            }
            return randomAccessFile.getFD();
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if(randomAccessFile != null) {
                randomAccessFile.close();
            }
        }
    }
}
