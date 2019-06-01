/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.quintans.ezSQL.toolkit.io;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.DeferredFileOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * <p>
 * This is an adaptation of org.apache.commons.fileupload.disk.DiskFileItem.java
 *
 * <p>
 * After retrieving an instance of this class, you may either request all contents of file at once using
 * {@link #get()} or request an {@link java.io.InputStream InputStream} with {@link #getInputStream()}
 * and process the file without attempting to load it into memory, which may
 * come handy with large files.
 *
 * <p>
 * Temporary files, which are created for file items, should be deleted later on. However, if you do use such a tracker,
 * then you must consider the following: Temporary files are automatically deleted as soon as they are no longer needed.
 * (More precisely, when the corresponding instance of {@link java.io.File} is garbage collected.)
 * </p>
 */
public class BinStore implements Serializable {

  // ----------------------------------------------------- Manifest constants

  /**
   * The UID to use when serializing this instance.
   */
  private static final long serialVersionUID = 2237570099615271025L;

  public static final int DEFAULT_THRESHOLD = 65535; // 65KB

  // ----------------------------------------------------------- Data members

  /**
   * The size of the item, in bytes. This is used to cache the size when a
   * file item is moved from its original location.
   */
  private long size = -1;

  /**
   * The threshold above which uploads will be stored on disk.
   */
  private int sizeThreshold = DEFAULT_THRESHOLD;

  /**
   * Cached contents of the file.
   */
  private byte[] cachedContent;

  /**
   * Output stream for this item.
   */
  private transient DeferredFileOutputStream dfos;

  /**
   * The temporary file to use.
   */
  private transient File tempFile;

  /**
   * File to allow for serialization of the content of this item.
   */
  private File dfosFile;

  public BinStore() {
  }

  public BinStore(byte[] data) {
    set(data);
  }

  public static BinStore of(byte[] data) {
    return new BinStore(data);
  }

  public static BinStore ofFile(String pathname) throws IOException {
    BinStore bs = new BinStore();
    bs.set(new File(pathname));
    return bs;
  }

  public static BinStore ofInputStream(InputStream inputStream) throws IOException {
    BinStore bs = new BinStore();
    bs.set(inputStream);
    return bs;
  }

  public BinStore(int sizeThreshold, byte[] data) {
    this(sizeThreshold);
    set(data);
  }

  /**
   * Constructs a new <code>BinStore</code> instance.
   *
   * @param sizeThreshold The threshold, in bytes, below which items will be
   *                      retained in memory and above which they will be
   *                      stored as a file.
   */
  public BinStore(int sizeThreshold) {
    this.sizeThreshold = sizeThreshold;
  }

  /**
   * Returns an {@link java.io.InputStream InputStream} that can be
   * used to retrieve the contents of the file.
   *
   * @return An {@link java.io.InputStream InputStream} that can be
   * used to retrieve the contents of the file.
   * @throws IOException if an error occurs.
   */
  public InputStream getInputStream() throws IOException {
    if (!isInMemory()) {
      return new FileInputStream(this.dfos.getFile());
    }

    if (this.cachedContent == null) {
      this.cachedContent = this.dfos.getData();
    }
    return new ByteArrayInputStream(this.cachedContent);
  }

  /**
   * Provides a hint as to whether or not the file contents will be read
   * from memory.
   *
   * @return <code>true</code> if the file contents will be read
   * from memory; <code>false</code> otherwise.
   */
  public boolean isInMemory() {
    if (this.cachedContent != null) {
      return true;
    }
    return this.dfos.isInMemory();
  }

  /**
   * Returns the size of the file.
   *
   * @return The size of the file, in bytes.
   */
  public long getSize() {
    if (this.size >= 0) {
      return this.size;
    } else if (this.cachedContent != null) {
      return this.cachedContent.length;
    } else if (this.dfos.isInMemory()) {
      return this.dfos.getData().length;
    } else {
      return this.dfos.getFile().length();
    }
  }

  /**
   * Returns the contents of the file as an array of bytes. If the
   * contents of the file were not yet cached in memory, they will be
   * loaded from the disk storage and cached.
   *
   * @return The contents of the file as an array of bytes.
   */
  public byte[] get() {
    if (isInMemory()) {
      if (this.cachedContent == null) {
        this.cachedContent = this.dfos.getData();
      }
      return this.cachedContent;
    }

    byte[] fileData = new byte[(int) getSize()];
    FileInputStream fis = null;

    try {
      fis = new FileInputStream(this.dfos.getFile());
      fis.read(fileData);
    } catch (IOException e) {
      fileData = null;
    } finally {
      IOUtils.closeQuietly(fis);
    }

    return fileData;
  }

  /**
   * Sets the contents using an array of bytes as a source
   *
   * @param data The array of bytes with the data
   */
  public void set(byte[] data) {
    try {
      this.cachedContent = null;
      copyAndClose(new ByteArrayInputStream(data), getOutputStream());
    } catch (IOException e) {
      // ignore
    }
  }

  /**
   * Sets the contents using a File as a source
   *
   * @param source The file with the data
   * @throws IOException
   */
  public void set(File source) throws IOException {
    set(new FileInputStream(source));
  }

  /**
   * Sets the contents using a InputStream as a source
   *
   * @param source The InputStream with the data
   * @throws IOException if something went wrong
   */
  public void set(InputStream source) throws IOException {
    OutputStream out = null;
    try {
      out = getOutputStream();
      IOUtils.copy(source, out);
    } finally {
      IOUtils.closeQuietly(out);
    }
  }

  /**
   * A convenience method to write an uploaded item to disk. The client code
   * is not concerned with whether or not the item is stored in memory, or on
   * disk in a temporary location. They just want to write the uploaded item
   * to a file.
   * <p>
   * This implementation first attempts to rename the uploaded item to the specified destination file, if the item was originally written to disk. Otherwise, the data will be
   * copied to the specified file.
   * <p>
   * This method is only guaranteed to work <em>once</em>, the first time it is invoked for a particular item. This is because, in the event that the method renames a temporary
   * file, that file will no longer be available to copy or rename again at a later time.
   *
   * @param file The <code>File</code> into which the uploaded item should
   *             be stored.
   * @throws Exception if an error occurs.
   */
  public void write(File file) throws Exception {
    if (isInMemory()) {
      FileOutputStream fout = null;
      try {
        fout = new FileOutputStream(file);
        fout.write(get());
      } finally {
        if (fout != null) {
          fout.close();
        }
      }
    } else {
      File outputFile = getStoreLocation();
      if (outputFile != null) {
        // Save the length of the file
        this.size = outputFile.length();
        /*
         * The uploaded file is being stored on disk
         * in a temporary location so move it to the
         * desired file.
         */
        if (!outputFile.renameTo(file)) {
          BufferedInputStream in = null;
          BufferedOutputStream out = null;
          try {
            in = new BufferedInputStream(
                new FileInputStream(outputFile));
            out = new BufferedOutputStream(
                new FileOutputStream(file));
            IOUtils.copy(in, out);
          } finally {
            if (in != null) {
              try {
                in.close();
              } catch (IOException e) {
                // ignore
              }
            }
            if (out != null) {
              try {
                out.close();
              } catch (IOException e) {
                // ignore
              }
            }
          }
        }
      } else {
        /*
         * For whatever reason we cannot write the
         * file to disk.
         */
        throw new IOException("Cannot write to disk!");
      }
    }
  }

  /**
   * Deletes the underlying storage for a file item, including deleting any
   * associated temporary disk file. Although this storage will be deleted
   * automatically when the <code>FileItem</code> instance is garbage
   * collected, this method can be used to ensure that this is done at an
   * earlier time, thus preserving system resources.
   */
  public void delete() {
    this.cachedContent = null;
    File outputFile = getStoreLocation();
    if (outputFile != null && outputFile.exists()) {
      outputFile.delete();
    }
  }

  public OutputStream getOutputStream() throws IOException {
    return getOutputStream(null);
  }

  /**
   * Returns an {@link java.io.OutputStream OutputStream} that can
   * be used for storing the contents of the file.
   *
   * @param directory The directory where the temporary file is created.<br>
   *                  If <code>null</code> it's created in the default directory.
   * @return An {@link java.io.OutputStream OutputStream} that can be used
   * for storing the contents of the file.
   * @throws IOException if an error occurs.
   */
  public OutputStream getOutputStream(File directory) throws IOException {
    this.dfos = new DeferredFileOutputStream(this.sizeThreshold, getTempFile(directory));
    return this.dfos;
  }

  /**
   * Returns the {@link java.io.File} object for the <code>FileItem</code>'s
   * data's temporary location on the disk. Note that for <code>FileItem</code>s that have their data stored in memory,
   * this method will return <code>null</code>. When handling large
   * files, you can use {@link java.io.File#renameTo(java.io.File)} to
   * move the file to new location without copying the data, if the
   * source and destination locations reside within the same logical
   * volume.
   *
   * @return The data file, or <code>null</code> if the data is stored in
   * memory.
   */
  public File getStoreLocation() {
    return this.dfos == null ? null : this.dfos.getFile();
  }

  /**
   * Removes the file contents from the temporary storage.
   */
  @Override
  protected void finalize() {
    File outputFile = this.dfos.getFile();

    if (outputFile != null && outputFile.exists()) {
      outputFile.delete();
    }
  }

  /**
   * Creates and returns a {@link java.io.File File} representing a uniquely
   * named temporary file in the configured repository path. The lifetime of
   * the file is tied to the lifetime of the <code>FileItem</code> instance;
   * the file will be deleted when the instance is garbage collected.
   *
   * @param directory The directory where this temporary file is created
   * @return The {@link java.io.File File} to be used for temporary storage.
   */
  protected File getTempFile(File directory) {
    if (this.tempFile == null) {
      try {
        if (directory != null && directory.isDirectory())
          this.tempFile = File.createTempFile("byteCache_", null, directory);
        else
          this.tempFile = File.createTempFile("byteCache_", null);
        this.tempFile.deleteOnExit();
      } catch (IOException e) {
      }
    }
    return this.tempFile;
  }

  private void copyAndClose(InputStream in, OutputStream out) throws IOException {
    try {
      IOUtils.copy(in, out);
    } finally {
      IOUtils.closeQuietly(in);
      IOUtils.closeQuietly(out);
    }
  }

  /**
   * Returns a string representation of this object.
   *
   * @return a string representation of this object.
   */
  @Override
  public String toString() {
    return "StoreLocation="
        + (isInMemory() ? "IN MEMORY" : String.valueOf(getStoreLocation()))
        + ", size="
        + getSize()
        + "bytes";
  }

  /**
   * Writes the state of this object during serialization.
   *
   * @param out The stream to which the state should be written.
   * @throws IOException if an error occurs.
   */
  private void writeObject(ObjectOutputStream out) throws IOException {
    // Read the data
    if (this.dfos.isInMemory()) {
      this.cachedContent = get();
    } else {
      this.cachedContent = null;
      this.dfosFile = this.dfos.getFile();
    }

    // write out values
    out.defaultWriteObject();
  }

  /**
   * Reads the state of this object during deserialization.
   *
   * @param in The stream from which the state should be read.
   * @throws IOException            if an error occurs.
   * @throws ClassNotFoundException if class cannot be found.
   */
  private void readObject(ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    // read values
    in.defaultReadObject();

    OutputStream output = getOutputStream();
    if (this.cachedContent != null) {
      output.write(this.cachedContent);
    } else {
      FileInputStream input = new FileInputStream(this.dfosFile);
      IOUtils.copy(input, output);
      this.dfosFile.delete();
      this.dfosFile = null;
    }
    output.close();

    this.cachedContent = null;
  }
}
