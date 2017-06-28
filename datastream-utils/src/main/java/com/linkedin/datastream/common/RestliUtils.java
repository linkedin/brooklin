package com.linkedin.datastream.common;

import com.linkedin.restli.server.PagingContext;

import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;


/**
 * Utility class to simplify usage of Restli.
 */
public final class RestliUtils {

  private static final String DEFAULT_URI_SCHEME = "http://";

  public static String sanitizeUri(String dmsUri) {
    return StringUtils.prependIfMissing(StringUtils.appendIfMissing(dmsUri, "/"), DEFAULT_URI_SCHEME);
  }

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
