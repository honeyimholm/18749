package edu.cmu.rds749.lab2;

import edu.cmu.rds749.common.AbstractProxy;
import edu.cmu.rds749.common.BankAccountStub;
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.apache.commons.configuration2.Configuration;
import rds749.BankAccount;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 *
 * Implements the Proxy.
 */
public class Proxy extends AbstractProxy
{
    List<BankAccountStub> registeredServers = new ArrayList<BankAccountStub>();
    List <Long> failedServers = new ArrayList<Long>();
    HashMap<Integer, List> duplicateSuppressionRecords = new HashMap<>();
    final ReentrantReadWriteLock registeredServerLock = new ReentrantReadWriteLock();

    public Proxy(Configuration config)
    {
        super(config);
    }

    @Override
    protected void serverRegistered(long id, BankAccountStub stub)
    {
        int activeBalance = -1;

        registeredServerLock.readLock().lock();
        if (this.registeredServers.isEmpty()) {
            activeBalance = 0;
        }
        registeredServerLock.readLock().unlock();


        while (activeBalance == -1) {
            try {
                registeredServerLock.readLock().lock();
                activeBalance = registeredServers.get(0).getState();
            } catch (BankAccountStub.NoConnectionException e) {
                System.out.println("Failed!");
                registeredServerLock.readLock().unlock();
                registeredServerLock.writeLock().lock();
                this.registeredServers.remove(0);
                registeredServerLock.writeLock().unlock();
            }
            finally {
                registeredServerLock.readLock().unlock();
            }
        }

        try {
            stub.setState(activeBalance);
        } catch (BankAccountStub.NoConnectionException e) {
            System.out.println("Couldn't set state...");
            // Throw an error!
        }

        registeredServerLock.writeLock().lock();
        this.registeredServers.add(stub);
        registeredServerLock.writeLock().unlock();

    }

    @Override
    protected synchronized void beginReadBalance(int reqid)
    {
        System.out.println("(In Proxy Begin Read Balance)");

        sendToAllServers(reqid, 0, true);
    }

    @Override
    protected synchronized void beginChangeBalance(int reqid, int update)
    {
        System.out.println("(In Proxy Begin Change Balance)");

        sendToAllServers(reqid, update, false);
    }

    @Override
    protected synchronized void endReadBalance(long serverid, int reqid, int balance)
    {
        System.out.println("(In Proxy End Read Balance)");
        if (this.duplicateSuppressionRecords.containsKey(reqid)) {
            this.duplicateSuppressionRecords.get(reqid).add(serverid);
        } else {
            List<Long> newReqidResponses = new ArrayList<>();
            newReqidResponses.add(serverid);
            this.duplicateSuppressionRecords.put(reqid, newReqidResponses);
        }
        if (this.duplicateSuppressionRecords.get(reqid).size() ==
                this.registeredServers.size()) {
            this.duplicateSuppressionRecords.remove(reqid);
            this.clientProxy.endReadBalance(reqid, balance);
        }
    }

    @Override
    protected synchronized void endChangeBalance(long serverid, int reqid, int balance)
    {
        System.out.println("(In Proxy End Change Balance)");
        if (this.duplicateSuppressionRecords.containsKey(reqid)) {
            this.duplicateSuppressionRecords.get(reqid).add(serverid);
        } else {
            List<Long> newReqidResponses = new ArrayList<>();
            newReqidResponses.add(serverid);
            this.duplicateSuppressionRecords.put(reqid, newReqidResponses);
        }
        if (this.duplicateSuppressionRecords.get(reqid).size() ==
                this.registeredServers.size()) {
            this.duplicateSuppressionRecords.remove(reqid);
            this.clientProxy.endChangeBalance(reqid, balance);
        }
    }

    @Override
    protected void serversFailed(List<Long> failedServers)
    {
        super.serversFailed(failedServers);
    }

    protected synchronized void sendToAllServers(int reqid, int update, boolean doRead) {
        if (this.registeredServers.isEmpty()) {
            this.clientProxy.RequestUnsuccessfulException(reqid);
            return;
        }

        List<BankAccountStub> failedServers = new ArrayList<>();

        for (BankAccountStub stub : this.registeredServers) {
            try {
                if (doRead) stub.beginReadBalance(reqid);
                else stub.beginChangeBalance(reqid, update);
            } catch (BankAccountStub.NoConnectionException e) {
                failedServers.add(stub);
            }
        }

        for (BankAccountStub stub : failedServers) {
            this.registeredServers.remove(stub);
        }

        if (this.registeredServers.isEmpty()) {
            this.clientProxy.RequestUnsuccessfulException(reqid);
        }
    }
}
