package edu.cmu.rds749.lab3;

import edu.cmu.rds749.common.AbstractProxy;
import edu.cmu.rds749.common.BankAccountStub;
import org.apache.commons.configuration2.Configuration;
import rds749.IncorrectOperation;

import java.util.*;


/**
 * Created by jiaqi on 8/28/16.
 *
 * Implements the Proxy.
 */
public class Proxy extends AbstractProxy
{
    BankAccountStub primaryServer = null;
    List<BankAccountStub> allServers = new ArrayList<BankAccountStub>();
    int checkpointFreq;

    public Proxy(Configuration config)
    {
        super(config);
        this.checkpointFreq = this.config.getInt("checkpointFrequency");
        //TODO:Set up timer event with checkpointing frequency to checkpoint from primaru!
    }

    @Override
    protected synchronized void serverRegistered(long id, BankAccountStub stub)
    {
        if (this.primaryServer == null) {
            try {
                stub.setPrimary();
                this.primaryServer = stub;
                this.allServers.add(stub);
            } catch (IncorrectOperation incorrectOperation) {
                System.out.println("Incorrect operation!");
            } catch (BankAccountStub.NoConnectionException e) {
                System.out.println("No connection exception... switch servers");
            }
        }
        else {
            try {
                stub.setBackup();
                this.allServers.add(stub);
            } catch (BankAccountStub.NoConnectionException e) {
                System.out.println("Incorrect operation!");
            } catch (IncorrectOperation incorrectOperation) {
                System.out.println("No connection exception... remove");
            }
        }
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

        //TODO: Handle response from primary and then backups

        /*if (this.duplicateSuppressionRecords.containsKey(reqid)) {
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
        }*/
    }

    @Override
    protected synchronized void endChangeBalance(long serverid, int reqid, int balance)
    {
        System.out.println("(In Proxy End Change Balance)");

        //TODO: Handle response from primary and then backups

        /*if (this.duplicateSuppressionRecords.containsKey(reqid)) {
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
        }*/
    }

    @Override
    protected synchronized void serversFailed(List<Long> failedServers)
    {
        super.serversFailed(failedServers);
    }

    protected synchronized void sendToAllServers(int reqid, int update, boolean doRead) {
        if (this.allServers.isEmpty()) {
            this.clientProxy.RequestUnsuccessfulException(reqid);
            return;
        }

        List<BankAccountStub> failedServers = new ArrayList<>();

        for (BankAccountStub stub : this.allServers) {
            try {
                if (doRead) stub.beginReadBalance(reqid);
                else stub.beginChangeBalance(reqid, update);
            } catch (BankAccountStub.NoConnectionException e) {
                failedServers.add(stub);
            }
        }

        for (BankAccountStub stub : failedServers) {
            if (stub == this.primaryServer) {
                // TODO: Trigger failover and select new primary from backups
                this.primaryServer = null;
                selectNewPrimary();
            }
            this.allServers.remove(stub);
        }

        if (this.allServers.isEmpty()) {
            this.clientProxy.RequestUnsuccessfulException(reqid);
        }
    }

    protected void selectNewPrimary() {
        // TODO:Go through backups and select a new working primary similar to lab1
    }
}
