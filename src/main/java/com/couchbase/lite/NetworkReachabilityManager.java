package com.couchbase.lite;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This uses system api (on Android, uses the Context) to listen for network reachability
 * change events and notifies all NetworkReachabilityListeners that have registered themselves.
 * (an example of a NetworkReachabilityListeners is a Replicator that wants to pause when
 * it's been detected that the network is not reachable)
 */
public abstract class NetworkReachabilityManager {

    protected List<NetworkReachabilityListener> listeners =
            new CopyOnWriteArrayList<NetworkReachabilityListener>();

    /**
     * Add Network Reachability Listener
     */
    public synchronized void addNetworkReachabilityListener(
            NetworkReachabilityListener listener) {
        int numListenersBeforeAdd = listeners.size();
        listeners.add(listener);
        if (numListenersBeforeAdd == 0)
            startListening();
    }

    /**
     * Remove Network Reachability Listener
     */
    public synchronized void removeNetworkReachabilityListener(
            NetworkReachabilityListener listener) {
        listeners.remove(listener);
        if (listeners.size() == 0)
            stopListening();
    }

    /**
     * Notify listeners that the network is now reachable
     */
    public synchronized void notifyListenersNetworkReachable() {
        for (NetworkReachabilityListener listener : listeners)
            listener.networkReachable();
    }

    /**
     * Notify listeners that the network is now unreachable
     */
    public synchronized void notifyListenersNetworkUneachable() {
        for (NetworkReachabilityListener listener : listeners)
            listener.networkUnreachable();
    }

    /**
     * This method starts listening for network connectivity state changes.
     */
    public abstract void startListening();

    /**
     * This method stops this class from listening for network changes.
     */
    public abstract void stopListening();

    public abstract boolean isOnline();
}