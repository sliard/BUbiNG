package it.unimi.di.law.bubing.frontier;

import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.URI;
import static it.unimi.di.law.bubing.tool.HostHash.hostLongHash;

public class BlackListing {
    private static final Logger LOGGER = LoggerFactory.getLogger(BlackListing.class);

    public static boolean checkBlacklistedIP(Frontier frontier, URI url, byte[] address)
    {
        String host = url.getHost();
        boolean blackListed = false;

        try {
            // We use Ints.fromBytes() as the array version generates an Object array just to log, possibly, a wrong argument (!)
            if (frontier.rc.blackListedIPv4Addresses.contains(Ints.fromBytes(address[0], address[1], address[2], address[3]))) {
                if (LOGGER.isDebugEnabled()) LOGGER.debug("URL {} disallowed by last-minute check for IP blacklisting", url);
                blackListed = true;
                }
            } catch (Exception e) {
                LOGGER.warn("Exception in blacklist Ip checking {}", host, e);
            }
        return blackListed;
    }

    public static boolean checkBlacklistedHost(Frontier frontier, URI url)
    {
        String host = url.getHost();
        boolean blackListed = false;

        try {
            int firstPoint = 0;

            do {
                LOGGER.debug("Testing {} for blacklisting", host);
                if (frontier.rc.blackListedHostHashes.contains(hostLongHash(host))) {
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("URL {} disallowed by last-minute check for host blacklisting", url);
                    blackListed = true;
                }
                firstPoint = host.indexOf('.') + 1; // firstPoint == 0 if not point found
                if (firstPoint > 0)
                    host = host.substring(firstPoint);
            } while ((!blackListed) && (firstPoint > 0));
        } catch (Exception e) {
            LOGGER.warn("Exception in blacklist Host checking {}", host, e);
        }
        return blackListed;
    }
}
