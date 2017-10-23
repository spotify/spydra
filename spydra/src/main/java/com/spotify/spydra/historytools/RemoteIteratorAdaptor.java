/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.spydra.historytools;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple adaptor for Hadoop's RemoteIterator to the Iterator interface to be able to
 * use all the nice libraries/functions around iterators.
 *
 * @param <T> Type of the objects this iterator yields
 */
public class RemoteIteratorAdaptor<T> implements RemoteIterator<T>, Iterator<T> {

  static final Logger logger = LoggerFactory.getLogger(RemoteIteratorAdaptor.class);
  private final RemoteIterator<T> wrappedRemoteIter;

  public RemoteIteratorAdaptor(RemoteIterator<T> remoteIterator) {
    this.wrappedRemoteIter = remoteIterator;
  }

  @Override
  public boolean hasNext() {
    try {
      return this.wrappedRemoteIter.hasNext();
    } catch (IOException e) {
      logger.error("Remote iterator failed checking for next element in remote iterator", e);
      throw new WrappedRemoteIteratorException(e);
    }
  }

  @Override
  public T next() {
    try {
      return this.wrappedRemoteIter.next();
    } catch (IOException e) {
      logger.error("Failed retrieving next element from remote iterator", e);
      throw new WrappedRemoteIteratorException(e);
    }
  }

  public static class WrappedRemoteIteratorException extends RuntimeException {
    public WrappedRemoteIteratorException(Throwable cause) {
      super(cause);
    }
  }
}
