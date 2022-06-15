package it.unimi.di.law.bubing.frontier;

import com.exensa.wdl.protobuf.crawler.EnumFetchStatus;
import it.unimi.di.law.bubing.util.TooSlowException;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.concurrent.TimeUnit;


public final class ExceptionHelper
{
  /** A map recording for each type of exception a timeout, Note that 0 means standard politeness time. */
  public static final Object2LongOpenHashMap<Class<?>> EXCEPTION_TO_WAIT_TIME = new Object2LongOpenHashMap<>();
  /** A map recording for each type of exception the number of retries. */
  public static final Object2LongOpenHashMap<Class<?>> EXCEPTION_TO_MAX_RETRIES = new Object2LongOpenHashMap<>();
  /** A map recording for each type of exception the fact that we can purge the host. */
  public static final ObjectOpenHashSet<Class<?>> EXCEPTION_HOST_KILLER = new ObjectOpenHashSet<>();
  /** A map recording for each type of exception the fact that we can purge the workbenchentry. */
  public static final ObjectOpenHashSet<Class<?>> EXCEPTION_ENTRY_KILLER = new ObjectOpenHashSet<>();
  /** A map recording for each type of exception the FetchStatus */
  public static final Object2IntOpenHashMap<Class<?>> EXCEPTION_TO_FETCH_STATUS = new Object2IntOpenHashMap<>();

  private static final long BASE_TIME = TimeUnit.MINUTES.toMillis(35);

  static {
    EXCEPTION_TO_WAIT_TIME.defaultReturnValue( BASE_TIME );
    EXCEPTION_TO_WAIT_TIME.put(java.net.NoRouteToHostException.class, 3 * BASE_TIME);
    EXCEPTION_TO_WAIT_TIME.put(java.net.SocketException.class, BASE_TIME);
    EXCEPTION_TO_WAIT_TIME.put(java.net.SocketTimeoutException.class, BASE_TIME);
    EXCEPTION_TO_WAIT_TIME.put(java.net.UnknownHostException.class, 3 * BASE_TIME);
    EXCEPTION_TO_WAIT_TIME.put(javax.net.ssl.SSLPeerUnverifiedException.class, 2 * BASE_TIME);

    EXCEPTION_TO_MAX_RETRIES.defaultReturnValue(2);
    EXCEPTION_TO_MAX_RETRIES.put(java.net.UnknownHostException.class, 3);
    EXCEPTION_TO_MAX_RETRIES.put(java.net.NoRouteToHostException.class, 2);
    EXCEPTION_TO_MAX_RETRIES.put(java.net.SocketException.class, 4);
    EXCEPTION_TO_MAX_RETRIES.put(java.net.SocketTimeoutException.class, 4);
    EXCEPTION_TO_MAX_RETRIES.put(javax.net.ssl.SSLPeerUnverifiedException.class, 1);
    EXCEPTION_TO_MAX_RETRIES.put(org.apache.http.client.CircularRedirectException.class, 1);
    EXCEPTION_TO_MAX_RETRIES.put(org.apache.http.client.RedirectException.class, 1);
    EXCEPTION_TO_MAX_RETRIES.put(org.apache.http.conn.ConnectTimeoutException.class, 1);
    EXCEPTION_TO_MAX_RETRIES.put(org.apache.http.ConnectionClosedException.class, 2);
    EXCEPTION_TO_MAX_RETRIES.put(org.apache.http.NoHttpResponseException.class, 2);
    EXCEPTION_TO_MAX_RETRIES.put(org.apache.http.TruncatedChunkException.class, 2);
    EXCEPTION_TO_MAX_RETRIES.put(org.apache.http.MalformedChunkCodingException.class, 2);

    EXCEPTION_HOST_KILLER.add(java.net.NoRouteToHostException.class);
    EXCEPTION_HOST_KILLER.add(java.net.UnknownHostException.class);
    EXCEPTION_HOST_KILLER.add(java.net.SocketException.class);
    EXCEPTION_HOST_KILLER.add(java.net.SocketTimeoutException.class);

    EXCEPTION_ENTRY_KILLER.add(java.net.NoRouteToHostException.class);



    EXCEPTION_TO_FETCH_STATUS.put(java.net.NoRouteToHostException.class, EnumFetchStatus.Enum.NO_ROUTE_TO_HOST_VALUE);
    EXCEPTION_TO_FETCH_STATUS.put(java.net.SocketException.class, EnumFetchStatus.Enum.SOCKET_ERROR_VALUE);
    EXCEPTION_TO_FETCH_STATUS.put(java.net.SocketTimeoutException.class, EnumFetchStatus.Enum.SOCKET_TIMEOUT_VALUE);
    EXCEPTION_TO_FETCH_STATUS.put(java.net.UnknownHostException.class, EnumFetchStatus.Enum.UNKNOWN_HOST_VALUE);
    EXCEPTION_TO_FETCH_STATUS.put(javax.net.ssl.SSLPeerUnverifiedException.class, EnumFetchStatus.Enum.SSL_ERROR_VALUE);
    EXCEPTION_TO_FETCH_STATUS.put(org.apache.http.client.CircularRedirectException.class, EnumFetchStatus.Enum.CIRCULAR_REDIRECT_ERROR_VALUE);
    EXCEPTION_TO_FETCH_STATUS.put(org.apache.http.client.RedirectException.class, EnumFetchStatus.Enum.REDIRECT_ERROR_VALUE);
    EXCEPTION_TO_FETCH_STATUS.put(org.apache.http.conn.ConnectTimeoutException.class, EnumFetchStatus.Enum.HTTP_CONNECTION_TIMEOUT_VALUE);
    EXCEPTION_TO_FETCH_STATUS.put(TooSlowException.class, EnumFetchStatus.Enum.HTTP_CONNECTION_TIMEOUT_VALUE);
    EXCEPTION_TO_FETCH_STATUS.put(org.apache.http.ConnectionClosedException.class, EnumFetchStatus.Enum.HTTP_CONNECTION_CLOSED_VALUE);
    EXCEPTION_TO_FETCH_STATUS.put(org.apache.http.conn.HttpHostConnectException.class, EnumFetchStatus.Enum.HTTP_HOST_CONNECT_ERROR_VALUE);
    EXCEPTION_TO_FETCH_STATUS.put(org.apache.http.NoHttpResponseException.class, EnumFetchStatus.Enum.HTTP_NO_HTTP_RESPONSE_VALUE);
    EXCEPTION_TO_FETCH_STATUS.put(org.apache.http.TruncatedChunkException.class, EnumFetchStatus.Enum.HTTP_TRUNCATED_CHUNK_VALUE);
    EXCEPTION_TO_FETCH_STATUS.put(org.apache.http.MalformedChunkCodingException.class, EnumFetchStatus.Enum.HTTP_MALFORMED_CHUNK_VALUE);

  }
}
