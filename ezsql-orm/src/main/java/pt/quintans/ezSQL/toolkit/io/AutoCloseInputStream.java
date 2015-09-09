package pt.quintans.ezSQL.toolkit.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class AutoCloseInputStream extends FilterInputStream {

    private boolean streamOpen = true;

    private boolean selfClosed = false;

    /**
     * Create a new auto closing stream for the provided connection
     *
     * @param in the input stream to read from
     * @param watcher   To be notified when the contents of the stream have been
     *  consumed.
     */
    public AutoCloseInputStream(final InputStream in) {
        super(in);
    }

    /**
     * Reads the next byte of data from the input stream.
     *
     * @throws IOException when there is an error reading
     * @return the character read, or -1 for EOF
     */
    public int read() throws IOException {
        int l = -1;

        if (isReadAllowed()) {
            // underlying stream not closed, go ahead and read.
            l = super.read();
            checkClose(l);
        }

        return l;
    }

    /**
     * Reads up to <code>len</code> bytes of data from the stream.
     *
     * @param b a <code>byte</code> array to read data into
     * @param off an offset within the array to store data
     * @param len the maximum number of bytes to read
     * @return the number of bytes read or -1 for EOF
     * @throws IOException if there are errors reading
     */
    public int read(byte[] b, int off, int len) throws IOException {
        int l = -1;

        if (isReadAllowed()) {
            l = super.read(b,  off,  len);
            checkClose(l);
        }

        return l;
    }

    /**
     * Reads some number of bytes from the input stream and stores them into the
     * buffer array b.
     *
     * @param b a <code>byte</code> array to read data into
     * @return the number of bytes read or -1 for EOF
     * @throws IOException if there are errors reading
     */
    public int read(byte[] b) throws IOException {
        int l = -1;

        if (isReadAllowed()) {
            l = super.read(b);
            checkClose(l);
        }
        return l;
    }

    /**
     * Close the stream, and also close the underlying stream if it is not
     * already closed.
     * @throws IOException If an IO problem occurs.
     */
    public void close() throws IOException {
        if (!selfClosed) {
            selfClosed = true;
            streamEnded();
        }
    }

    /**
     * Close the underlying stream should the end of the stream arrive.
     *
     * @param readResult    The result of the read operation to check.
     * @throws IOException If an IO problem occurs.
     */
    private void checkClose(int readResult) throws IOException {
        if (readResult == -1) {
            streamEnded();
        }
    }

    /**
     * See whether a read of the underlying stream should be allowed, and if
     * not, check to see whether our stream has already been closed!
     *
     * @return <code>true</code> if it is still OK to read from the stream.
     * @throws IOException If an IO problem occurs.
     */
    private boolean isReadAllowed() throws IOException {
        if (!streamOpen && selfClosed) {
            throw new IOException("Attempted read on closed stream.");
        }
        return streamOpen;
    }

    /**
     * Notify the watcher that the contents have been consumed.
     * @throws IOException If an IO problem occurs.
     */
    private void streamEnded() throws IOException {
        if (streamOpen) {
            super.close();
            streamOpen = false;
        }
    }
}

