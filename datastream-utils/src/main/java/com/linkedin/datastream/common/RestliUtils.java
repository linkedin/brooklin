package com.linkedin.datastream.common;

import com.linkedin.restli.server.PagingContext;

import java.util.stream.Stream;

/**
 * Utility class to simplify usage of Restli.
 */
public final class RestliUtils {

  /**
   * Applies a Paging Context to a Stream.
   * @param stream stream of elements to be paginated
   * @param pagingContext settings for pagination
   * @param <T> type of elements in the Stream
   * @return
   */
  public static <T> Stream<T> withPaging(Stream<T> stream, final PagingContext pagingContext) {
    if (pagingContext.hasStart()) {
      stream = stream.skip(pagingContext.getStart());
    }

    if (pagingContext.hasCount()) {
      stream = stream.limit(pagingContext.getCount());
    }

    return stream;
  }
}
