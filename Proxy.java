package edu.cmu.rds749.lab2;

import edu.cmu.rds749.common.AbstractProxy;
import edu.cmu.rds749.common.BankAccountStub;
import org.apache.commons.configuration2.Configuration;
import rds749.BankAccount;

import java.util.ArrayList;
import java.util.List;


/**
 *
 * Implements the Proxy.
 */
public class Proxy extends AbstractProxy
{
    List<BankAccountStub> registeredServers = new ArrayList<BankAccountStub>();

    public Proxy(Configuration config)
    {
        super(config);
    }

    @Override
    protected void serverRegistered(long id, BankAccountStub stub)
    {
        int activeBalance = -1;

        if (this.registeredServers.isEmpty()) {
            activeBalance = 0;
        }

        while (activeBalance == -1) {
            try {
                activeBalance = registeredServers.get(0).getState();
            } catch (BankAccountStub.NoConnectionException e) {
                System.out.println("Failed!");
                this.registeredServers.remove(stub);
            }
        }

        try {
            stub.setState(activeBalance);
        } catch (BankAccountStub.NoConnectionException e) {
            System.out.println("Couldn't set state...");
            // Throw an error!
        }
        registeredServers.add(stub);
    }

    @Override
    protected void beginReadBalance(int reqid)
    {
        System.out.println("(In Proxy Begin Read Balance)");

        boolean success = false;

        while (!success) {
            if (this.registeredServers.isEmpty()) {
                // No more servers!
            }
            try {
                this.registeredServers.get(0).beginReadBalance(reqid);
                success = true;
            } catch (BankAccountStub.NoConnectionException e) {
                System.out.println("Failed!");
                this.registeredServers.remove(0);
            }
        }
    }

    @Override
    protected void beginChangeBalance(int reqid, int update)
    {
        System.out.println("(In Proxy Begin Change Balance)");
        for (BankAccountStub stub : this.registeredServers) {
            try {
                stub.beginChangeBalance(reqid, update);
            } catch (BankAccountStub.NoConnectionException e) {
                System.out.println("Failed!");
                this.registeredServers.remove(stub);
            }
        }

    }

    @Override
    protected void endReadBalance(long serverid, int reqid, int balance)
    {
        System.out.println("(In Proxy End Read Balance)");
        this.clientProxy.endReadBalance(reqid, balance);
    }

    @Override
    protected void endChangeBalance(long serverid, int reqid, int balance)
    {
        System.out.println("(In Proxy End Change Balance)");
        this.clientProxy.endChangeBalance(reqid, balance);
    }

    @Override
    protected void serversFailed(List<Long> failedServers)
    {
        super.serversFailed(failedServers);
    }
}
