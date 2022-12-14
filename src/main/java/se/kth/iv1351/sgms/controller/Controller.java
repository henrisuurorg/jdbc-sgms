/*
 * The MIT License (MIT)
 * Copyright (c) 2020 Leif Lindbäck
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction,including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so,subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package se.kth.iv1351.sgms.controller;

import java.util.ArrayList;
import java.util.List;

import se.kth.iv1351.sgms.integration.SchoolDAO;
import se.kth.iv1351.sgms.integration.SchoolDBException;
import se.kth.iv1351.sgms.model.*;

/**
 * This is the application's only controller, all calls to the model pass here.
 * The controller is also responsible for calling the DAO. Typically, the
 * controller first calls the DAO to retrieve data (if needed), then operates on
 * the data, and finally tells the DAO to store the updated data (if any).
 */
public class Controller {
    private final SchoolDAO schoolDb;

    /**
     * Creates a new instance, and retrieves a connection to the database.
     * 
     * @throws SchoolDBException If unable to connect to the database.
     */
    public Controller() throws SchoolDBException {
        schoolDb = new SchoolDAO();
    }

    /**
     * Creates a new account for the specified account holder.
     * 
     * @param holderName The account holder's name.
     * @throws InstrumentException If unable to create account.
     */
    public void createAccount(String holderName) throws InstrumentException {
        String failureMsg = "Could not create account for: " + holderName;

        if (holderName == null) {
            throw new InstrumentException(failureMsg);
        }

        try {
            schoolDb.createAccount(new Account(holderName));
        } catch (Exception e) {
            throw new InstrumentException(failureMsg, e);
        }
    }

    /**
     * Lists all accounts in the whole bank.
     * 
     * @return A list containing all accounts. The list is empty if there are no
     *         accounts.
     * @throws InstrumentException If unable to retrieve accounts.
     */
    public List<? extends InstrumentDTO> getAllInstruments() throws InstrumentException {
        try {
            return schoolDb.findAllInstruments();
        } catch (Exception e) {
            throw new InstrumentException("Unable to list instruments.", e);
        }
    }

    public List<? extends InstrumentDTO> getInstrumentForType(String instrument) throws InstrumentException {
        if (instrument == null) {
            return new ArrayList<>();
        }
        try {
            return schoolDb.findInstrumentsByType(instrument);
        } catch (Exception e) {
            throw new InstrumentException("Could not search for instrument.", e);
        }
    }

    /**
     * Retrieves the account with the specified number.
     * 
     * @param acctNo The number of the searched account.
     * @return The account with the specified account number, or <code>null</code>
     *         if there is no such account.
     * @throws InstrumentException If unable to retrieve the account.
     */
    public AccountDTO getAccount(String acctNo) throws InstrumentException {
        if (acctNo == null) {
            return null;
        }

        try {
            return schoolDb.findAccountByAcctNo(acctNo, false);
        } catch (Exception e) {
            throw new InstrumentException("Could not search for account.", e);
        }
    }

    /**
     * Deposits the specified amount to the account with the specified account
     * number.
     * 
     * @param acctNo The number of the account to which to deposit.
     * @param amt    The amount to deposit.
     * @throws RejectedException If not allowed to deposit the specified amount.
     * @throws InstrumentException  If failed to deposit.
     */
    public void deposit(String acctNo, int amt) throws RejectedException, InstrumentException {
        String failureMsg = "Could not deposit to account: " + acctNo;

        if (acctNo == null) {
            throw new InstrumentException(failureMsg);
        }

        try {
            Account acct = schoolDb.findAccountByAcctNo(acctNo, true);
            acct.deposit(amt);
            schoolDb.updateAccount(acct);
        } catch (SchoolDBException bdbe) {
            throw new InstrumentException(failureMsg, bdbe);
        } catch (Exception e) {
            commitOngoingTransaction(failureMsg);
            throw e;
        }
    }

    /**
     * Withdraws the specified amount from the account with the specified account
     * number.
     * 
     * @param acctNo The number of the account from which to withdraw.
     * @param amt    The amount to withdraw.
     * @throws RejectedException If not allowed to withdraw the specified amount.
     * @throws InstrumentException  If failed to withdraw.
     */
    public void withdraw(String acctNo, int amt) throws RejectedException, InstrumentException {
        String failureMsg = "Could not withdraw from account: " + acctNo;

        if (acctNo == null) {
            throw new InstrumentException(failureMsg);
        }

        try {
            Account acct = schoolDb.findAccountByAcctNo(acctNo, true);
            acct.withdraw(amt);
            schoolDb.updateAccount(acct);
        } catch (SchoolDBException bdbe) {
            throw new InstrumentException(failureMsg, bdbe);
        } catch (Exception e) {
            commitOngoingTransaction(failureMsg);
            throw e;
        }
    }

    public void rent(String instrumentId, String studentId) throws RejectedException, InstrumentException {
        String failureMsg = "Could not rent instrument: " + instrumentId;

        if (instrumentId == null || studentId == null) {
            throw new InstrumentException(failureMsg);
        }

        try {
            Instrument inst = schoolDb.findInstrumentById(instrumentId, false);
            Student student = schoolDb.findStudentById(studentId, false);

            ArrayList<RentalAgreement> activeRentalsForStudent = schoolDb.findActiveRentalsForStudent(studentId, false);
            student.addRentals(activeRentalsForStudent);
            RentalAgreement newRental = student.rent(inst.getRentalInstrumentId());

            schoolDb.createRentalAgreement(newRental);
        } catch (SchoolDBException bdbe) {
            throw new InstrumentException(failureMsg, bdbe);
        } catch (Exception e) {
            commitOngoingTransaction(failureMsg);
            throw e;
        }
    }

    private void commitOngoingTransaction(String failureMsg) throws InstrumentException {
        try {
            schoolDb.commit();
        } catch (SchoolDBException bdbe) {
            throw new InstrumentException(failureMsg, bdbe);
        }
    }

    /**
     * Deletes the account with the specified account number.
     * 
     * @param acctNo The number of the account that shall be deleted.
     * @throws InstrumentException If failed to delete the specified account.
     */
    public void deleteAccount(String acctNo) throws InstrumentException {
        String failureMsg = "Could not delete account: " + acctNo;

        if (acctNo == null) {
            throw new InstrumentException(failureMsg);
        }

        try {
            schoolDb.deleteAccount(acctNo);
        } catch (Exception e) {
            throw new InstrumentException(failureMsg, e);
        }
    }

    public void test(String instrumentId) throws SchoolDBException {
        Instrument inst = schoolDb.findInstrumentById(instrumentId, false);
        System.out.println(inst.getInstrument());
    }
}
