package com.datastax.driver.core;

import java.net.InetAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledFuture;

/**
 * A Cassandra node.
 *
 * This class keeps the informations the driver maintain on a given Cassandra node.
 */
public class Host {

    private final InetAddress address;
    private final HealthMonitor monitor;

    private volatile String datacenter;
    private volatile String rack;

    // Tracks reconnection attempts to that host so we avoid adding multiple tasks
    final AtomicReference<ScheduledFuture> reconnectionAttempt = new AtomicReference<ScheduledFuture>();

    // ClusterMetadata keeps one Host object per inet address, so don't use
    // that constructor unless you know what you do (use ClusterMetadata.getHost typically).
    Host(InetAddress address, ConvictionPolicy.Factory policy) {
        if (address == null || policy == null)
            throw new NullPointerException();

        this.address = address;
        this.monitor = new HealthMonitor(policy.create(this));
    }

    void setLocationInfo(String datacenter, String rack) {
        this.datacenter = datacenter;
        this.rack = rack;
    }

    /**
     * Returns the node address.
     *
     * @return the node {@link InetAddress}.
     */
    public InetAddress getAddress() {
        return address;
    }

    /**
     * Returns the name of the datacenter this host is part of.
     *
     * The returned datacenter name is the one as known by Cassandra. Also note
     * that it is possible for this information to not be available. In that
     * case this method returns {@code null} and caller should always expect
     * that possibility.
     *
     * @return the Cassandra datacenter name.
     */
    public String getDatacenter() {
        return datacenter;
    }

    /**
     * Returns the name of the rack this host is part of.
     *
     * The returned rack name is the one as known by Cassandra. Also note that
     * it is possible for this information to not be available. In that case
     * this method returns {@code null} and caller should always expect that
     * possibility.
     *
     * @return the Cassandra rack name.
     */
    public String getRack() {
        return rack;
    }

    /**
     * Returns the health monitor for this host.
     *
     * The health monitor keeps tracks of the known host state (up or down). A
     * class implementing {@link Host.StateListener} can also register against
     * the healt monitor to be notified when this node is detected down/up.
     *
     * @return the host {@link HealthMonitor}.
     */
    public HealthMonitor getMonitor() {
        return monitor;
    }

    @Override
    public final int hashCode() {
        return address.hashCode();
    }

    @Override
    public final boolean equals(Object o) {
        if(!(o instanceof Host))
            return false;

        return address.equals(((Host)o).address);
    }

    @Override
    public String toString() {
        return address.toString();
    }

    /**
     * Tracks the health of a node and notify listeners when a host is considered up or down.
     */
    public class HealthMonitor {

        private final ConvictionPolicy policy;

        private Set<Host.StateListener> listeners = new CopyOnWriteArraySet<Host.StateListener>();
        private volatile boolean isUp;

        HealthMonitor(ConvictionPolicy policy) {
            this.policy = policy;
            this.isUp = true;
        }

        /**
         * Register the provided listener to be notified on up/down events.
         *
         * Registering the same listener multiple times is a no-op.
         *
         * @param listener the new {@link Host.StateListener} to register.
         */
        public void register(StateListener listener) {
            listeners.add(listener);
        }

        /**
         * Unregister a given provided listener.
         *
         * This method is a no-op if {@code listener} hadn't previously be
         * registered against this monitor.
         *
         * @param listener the {@link Host.StateListener} to unregister.
         */
        public void unregister(StateListener listener) {
            listeners.add(listener);
        }

        /**
         * Returns whether the host is considered up by this monitor.
         *
         * @return whether the node is considered up.
         */
        public boolean isUp() {
            return isUp;
        }

        // TODO: Should we bother making sure that multiple calls to this don't inform the listeners twice?
        private void setDown() {
            isUp = false;
            for (Host.StateListener listener : listeners)
                listener.onDown(Host.this);
        }

        /**
         * Reset the monitor, setting the host as up and informing the
         * registered listener that the node is up.
         */
        void reset() {
            isUp = true;
            policy.reset();
            for (Host.StateListener listener : listeners)
                listener.onUp(Host.this);
        }

        boolean signalConnectionFailure(ConnectionException exception) {
            boolean isDown = policy.addFailure(exception);
            if (isDown)
                setDown();
            return isDown;
        }
    }

    /**
     * Interface for listener that are interested in hosts add, up, down and
     * remove events.
     */
    public interface StateListener {

        /**
         * Called when a new node is added to the cluster.
         *
         * The newly added node should be considered up.
         *
         * @param host the host that has been newly added.
         */
        public void onAdd(Host host);

        /**
         * Called when a node is detected up.
         *
         * @param host the host that has been detected up.
         */
        public void onUp(Host host);

        /**
         * Called when a node is detected down.
         *
         * @param host the host that has been detected down.
         */
        public void onDown(Host host);

        /**
         * Called when a node is remove from the cluster.
         *
         * @param host the removed host.
         */
        public void onRemove(Host host);
    }
}
