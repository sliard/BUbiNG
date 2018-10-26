package it.unimi.di.law.bubing.frontier;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.concurrent.TimeUnit;


public final class ExceptionHelper
{
  /** A map recording for each type of exception a timeout, Note that 0 means standard politeness time. */
  public static final Object2LongOpenHashMap<Class<?>> EXCEPTION_TO_WAIT_TIME = new Object2LongOpenHashMap<>();
  /** A map recording for each type of exception the number of retries. */
  public static final Object2IntOpenHashMap<Class<?>> EXCEPTION_TO_MAX_RETRIES = new Object2IntOpenHashMap<>();
  /** A map recording for each type of exception the number of retries. */
  public static final ObjectOpenHashSet<Class<?>> EXCEPTION_HOST_KILLER = new ObjectOpenHashSet<>();

  static {
    EXCEPTION_TO_WAIT_TIME.defaultReturnValue( TimeUnit.HOURS.toMillis(1));
    EXCEPTION_TO_WAIT_TIME.put(java.net.NoRouteToHostException.class, TimeUnit.HOURS.toMillis(1));
    EXCEPTION_TO_WAIT_TIME.put(java.net.SocketException.class, TimeUnit.MINUTES.toMillis(1));
    EXCEPTION_TO_WAIT_TIME.put(java.net.SocketTimeoutException.class, TimeUnit.MINUTES.toMillis(1));
    //EXCEPTION_TO_WAIT_TIME.put(java.net.SocketTimeoutException.class, 10000);
    EXCEPTION_TO_WAIT_TIME.put(java.net.UnknownHostException.class, TimeUnit.HOURS.toMillis(1));
    //EXCEPTION_TO_WAIT_TIME.put(java.net.UnknownHostException.class, 5000);
    EXCEPTION_TO_WAIT_TIME.put(javax.net.ssl.SSLPeerUnverifiedException.class, TimeUnit.HOURS.toMillis(1));
    EXCEPTION_TO_WAIT_TIME.put(org.apache.http.client.CircularRedirectException.class, 0);
    EXCEPTION_TO_WAIT_TIME.put(org.apache.http.client.RedirectException.class, 0);
    EXCEPTION_TO_WAIT_TIME.put(org.apache.http.conn.ConnectTimeoutException.class, TimeUnit.HOURS.toMillis(1));
    //EXCEPTION_TO_WAIT_TIME.put(org.apache.http.conn.ConnectTimeoutException.class, 20000);
    EXCEPTION_TO_WAIT_TIME.put(org.apache.http.ConnectionClosedException.class, TimeUnit.MINUTES.toMillis(1));
    EXCEPTION_TO_WAIT_TIME.put(org.apache.http.conn.HttpHostConnectException.class, TimeUnit.HOURS.toMillis(1));
    EXCEPTION_TO_WAIT_TIME.put(org.apache.http.NoHttpResponseException.class, TimeUnit.MINUTES.toMillis(1));
    EXCEPTION_TO_WAIT_TIME.put(org.apache.http.TruncatedChunkException.class, TimeUnit.MINUTES.toMillis(1));
    EXCEPTION_TO_WAIT_TIME.put(org.apache.http.MalformedChunkCodingException.class, TimeUnit.MINUTES.toMillis(1));

    EXCEPTION_TO_MAX_RETRIES.defaultReturnValue(0);
    EXCEPTION_TO_MAX_RETRIES.put(java.net.UnknownHostException.class, 0);
    EXCEPTION_TO_MAX_RETRIES.put(javax.net.ssl.SSLPeerUnverifiedException.class, 0);
    EXCEPTION_TO_MAX_RETRIES.put(org.apache.http.client.CircularRedirectException.class, 0);
    EXCEPTION_TO_MAX_RETRIES.put(org.apache.http.client.RedirectException.class, 0);
    EXCEPTION_TO_MAX_RETRIES.put(org.apache.http.conn.ConnectTimeoutException.class, 0);
    EXCEPTION_TO_MAX_RETRIES.put(org.apache.http.ConnectionClosedException.class, 0);
    EXCEPTION_TO_MAX_RETRIES.put(org.apache.http.NoHttpResponseException.class, 0);
    EXCEPTION_TO_MAX_RETRIES.put(org.apache.http.TruncatedChunkException.class, 0);
    EXCEPTION_TO_MAX_RETRIES.put(org.apache.http.MalformedChunkCodingException.class, 0);

    EXCEPTION_HOST_KILLER.add(java.net.NoRouteToHostException.class);
    EXCEPTION_HOST_KILLER.add(java.net.UnknownHostException.class);
    EXCEPTION_HOST_KILLER.add(java.net.SocketException.class);
    EXCEPTION_HOST_KILLER.add(javax.net.ssl.SSLPeerUnverifiedException.class);
    EXCEPTION_HOST_KILLER.add(org.apache.http.conn.ConnectTimeoutException.class);
  }
}
