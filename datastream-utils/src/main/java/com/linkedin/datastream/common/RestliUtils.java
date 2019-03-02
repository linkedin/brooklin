/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

import com.linkedin.restli.server.PagingContext;


/**
 * Utility class to simplify usage of Restli.
 */
public final class RestliUtils {
  private static final String URI_SCHEME = "http://";
  private static final String URI_SCHEME_SSL = "https://";

  public static String sanitizeUri(String uri) {
    uri = uri.toLowerCase();
    if (!uri.startsWith(URI_SCHEME) && !uri.startsWith(URI_SCHEME_SSL)) {
      // use plaintext by default
      uri = URI_SCHEME + uri;
    }
    return StringUtils.appendIfMissing(uri, "/");
  }

  /**
   * Applies a Paging Context to a Stream.
   * @param stream stream of elements to be paginated
   * @param pagingContext settings for pagination
   * @param <T> type of elements in the Stream
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
