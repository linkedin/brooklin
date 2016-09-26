package com.linkedin.datastream.common;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.restli.server.PagingContext;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;


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

  /**
   * Helper method to call an async method and wait for result.
   * @param method method lambda
   * @param opDesc operation description (tip: starts with a verb)
   * @param timeoutMs wait timeout in miliseconds
   * @param logger
   */
  public static void callAsyncAndWait(Consumer<Callback<None>> method, String opDesc, long timeoutMs, Logger logger) {
    Validate.notNull(method);
    Validate.notNull(opDesc);
    Validate.notNull(logger);
    Validate.isTrue(timeoutMs >= 0);

    try {
      FutureCallback<None> future = new FutureCallback<None>();
      method.accept(future);
      logger.info("Waiting to " + opDesc);
      future.get(timeoutMs, TimeUnit.MILLISECONDS);
      logger.info("It is successful to " + opDesc);
    } catch (Exception e) {
      String msg = "Failed to " + opDesc;
      logger.error(msg, e);
      throw new DatastreamRuntimeException(msg, e);
    }
  }

}
