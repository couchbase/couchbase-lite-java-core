package com.couchbase.lite;

import java.util.ArrayList;
import java.util.List;

/**
 * This uses system api (on Android, uses the Context) to listen for network reachability
 * change events and notifies all NetworkReachabilityListeners that have registered themselves.
 * (an example of a NetworkReachabilityListeners is a Replicator that wants to pause when
 * it's been detected that the network is not reachable)
 */
public abstract class NetworkReachabilityManager {

    protected List<NetworkReachabilityListener> networkReachabilityListeners;

    /**
     * Add Network Reachability Listener
     */
    public synchronized void addNetworkReachabilityListener(NetworkReachabilityListener listener) {
        if (networkReachabilityListeners == null) {
            networkReachabilityListeners = new ArrayList<NetworkReachabilityListener>();
        }
        int numListenersBeforeAdd = networkReachabilityListeners.size();
        networkReachabilityListeners.add(listener);
        if (numListenersBeforeAdd == 0) {
            startListening();
        }
    }

    /**
     * Remove Network Reachability Listener
     */
    public synchronized void removeNetworkReachabilityListener(NetworkReachabilityListener listener) {
        if (networkReachabilityListeners == null) {
            networkReachabilityListeners = new ArrayList<NetworkReachabilityListener>();
        }
        networkReachabilityListeners.remove(listener);
        if (networkReachabilityListeners.size() == 0) {
            stopListening();
        }
    }

    /**
     * Notify listeners that the network is now reachable
     */
    public synchronized void notifyListenersNetworkReachable() {

        for (NetworkReachabilityListener networkReachabilityListener : networkReachabilityListeners) {
            networkReachabilityListener.networkReachable();
        }
    }

    /**
     * Notify listeners that the network is now unreachable
     */
    public synchronized void notifyListenersNetworkUneachable() {
        for (NetworkReachabilityListener networkReachabilityListener : networkReachabilityListeners) {
            networkReachabilityListener.networkUnreachable();
        }
    }

    /**
     * This method starts listening for network connectivity state changes.
     */
    public abstract void startListening();

    /**
     * This method stops this class from listening for network changes.
     */
    public abstract void stopListening();

}