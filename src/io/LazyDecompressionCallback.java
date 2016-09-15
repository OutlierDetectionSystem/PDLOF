package io;

import java.io.IOException;


/**
 * Used to call back lazy decompression process. 
 * 
 * @see #BytesRefWritable
 */
public interface LazyDecompressionCallback {

  public byte[] decompress() throws IOException;

}
