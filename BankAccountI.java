package edu.cmu.rds749.lab3;

import edu.cmu.rds749.common.AbstractServer;
import org.apache.commons.configuration2.Configuration;
import rds749.Checkpoint;

import java.util.ArrayList;
import java.util.List;

/**
 * Implements the BankAccounts transactions interface
 * Created by utsav on 8/27/16.
 */

public class BankAccountI extends AbstractServer
{
    class LogEntry {
        public METHODS method;
        public int amount;
        public int reqid;
    };


    private int balance = 0;
    private boolean isPrimary = false;
    private ProxyControl ctl;
    private Checkpoint checkpoint = null;
    private List<LogEntry> log = new ArrayList<LogEntry>();

    public BankAccountI(Configuration config) {
        super(config);
    }

    @Override
    protected void doStart(ProxyControl ctl) throws Exception {
        this.ctl = ctl;
    }

    @Override
    protected void handleBeginReadBalance(int reqid)
    {
        LogEntry newEntry = new LogEntry();
        newEntry.method = METHODS.READ_BALANCE;
        newEntry.amount = -1;
        newEntry.reqid = reqid;
        this.log.add(newEntry);

        //this.ctl.endReadBalance(reqid, this.balance);
    }

    @Override
    protected void handleBeginChangeBalance(int reqid, int update)
    {
        this.balance += update;

        LogEntry newEntry = new LogEntry();
        newEntry.method = METHODS.CHANGE_BALANCE;
        newEntry.amount = update;
        newEntry.reqid = reqid;
        this.log.add(newEntry);

        //this.ctl.endChangeBalance(reqid, this.balance);
    }

    @Override
    protected Checkpoint handleGetState()
    {
        return new Checkpoint();
    }

    @Override
    protected int handleSetState(Checkpoint checkpoint)
    {
        return 0;
    }

    @Override
    protected void handleSetPrimary()
    {
        this.isPrimary = true;
    }

    @Override
    protected void handleSetBackup()
    {
        // Do nothing because isPrimary is default false
    }

    private enum METHODS {
        READ_BALANCE,
        CHANGE_BALANCE;
    }
}