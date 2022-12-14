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
// HEEEEEEy
package se.kth.iv1351.sgms.integration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import se.kth.iv1351.sgms.model.*;

/**
 * This data access object (DAO) encapsulates all database calls in the bank
 * application. No code outside this class shall have any knowledge about the
 * database.
 */
public class SchoolDAO {
    private static final String HOLDER_TABLE_NAME = "holder";
    private static final String HOLDER_PK_COLUMN_NAME = "holder_id";
    private static final String HOLDER_COLUMN_NAME = "name";
    private static final String ACCT_TABLE_NAME = "account";
    private static final String ACCT_NO_COLUMN_NAME = "account_no";
    private static final String BALANCE_COLUMN_NAME = "balance";
    private static final String HOLDER_FK_COLUMN_NAME = HOLDER_PK_COLUMN_NAME;

    private static final String INSTRUMENT_FEE_TABLE_NAME = "instrument_fee";
    private static final String RENTAL_AGREEMENT_TABLE_NAME = "rental_agreement";
    private static final String RENTAL_AGREEMENT_DATE_RETURNED_COLUMN_NAME = "date_returned";
    private static final String INSTRUMENT_TABLE_NAME = "rental_instrument";
    private static final String INSTRUMENT_INSTRUMENT_COLUMN_NAME = "instrument";
    private static final String INSTRUMENT_PK_COLUMN_NAME = "rental_instrument_id";
    private static final String INSTRUMENT_BRAND_COLUMN_NAME = "brand";
    private static final String INSTRUMENT_CATEGORY_COLUMN_NAME = "category";
    private static final String INSTRUMENT_FEE_COLUMN_NAME = "fee";
    private static final String STUDENT_PK_COLUMN_NAME = "student_id";
    private static final String STUDENT_NAME_COLUMN_NAME = "name";


    private Connection connection;
    private PreparedStatement createHolderStmt;
    private PreparedStatement findHolderPKStmt;
    private PreparedStatement createAccountStmt;
    private PreparedStatement findInstrumentsByTypeStmt;
    private PreparedStatement findAccountByAcctNoStmt;
    private PreparedStatement findAccountByAcctNoStmtLockingForUpdate;
    private PreparedStatement findAllInstrumentsStmt;
    private PreparedStatement deleteAccountStmt;
    private PreparedStatement changeBalanceStmt;
    private PreparedStatement findInstrumentByIdStmtLockingForUpdate;
    private PreparedStatement findInstrumentByIdStmt;
    private PreparedStatement findStudentByIdStmtLockingForUpdate;
    private PreparedStatement findStudentByIdStmt;
    private PreparedStatement findActiveRentalsForStudentStmtLockingForUpdate;
    private PreparedStatement findActiveRentalsForStudentStmt;
    private PreparedStatement createRentalAgreementStmt;


    /**
     * Constructs a new DAO object connected to the bank database.
     */
    public SchoolDAO() throws SchoolDBException {
        try {
            connectToSgmsDB();
            prepareStatements();
        } catch (ClassNotFoundException | SQLException exception) {
            throw new SchoolDBException("Could not connect to datasource.", exception);
        }
    }

    /**
     * Creates a new account.
     *
     * @param account The account to create.
     * @throws SchoolDBException If failed to create the specified account.
     */
    public void createAccount(AccountDTO account) throws SchoolDBException {
        String failureMsg = "Could not create the account: " + account;
        int updatedRows = 0;
        try {
            int holderPK = findHolderPKByName(account.getHolderName());
            if (holderPK == 0) {
                createHolderStmt.setString(1, account.getHolderName());
                updatedRows = createHolderStmt.executeUpdate();
                if (updatedRows != 1) {
                    handleException(failureMsg, null);
                }
                holderPK = findHolderPKByName(account.getHolderName());
            }

            createAccountStmt.setInt(1, createAccountNo());
            createAccountStmt.setInt(2, account.getBalance());
            createAccountStmt.setInt(3, holderPK);
            updatedRows = createAccountStmt.executeUpdate();
            if (updatedRows != 1) {
                handleException(failureMsg, null);
            }

            connection.commit();
        } catch (SQLException sqle) {
            handleException(failureMsg, sqle);
        }
    }

    public Account findAccountByAcctNo(String acctNo, boolean lockExclusive)
                                       throws SchoolDBException {
    PreparedStatement stmtToExecute;
        if (lockExclusive) {
            stmtToExecute = findAccountByAcctNoStmtLockingForUpdate;
        } else {
            stmtToExecute = findAccountByAcctNoStmt;
        }

        String failureMsg = "Could not search for specified account.";
        ResultSet result = null;
        try {
            stmtToExecute.setString(1, acctNo);
            result = stmtToExecute.executeQuery();
            if (result.next()) {
                return new Account(result.getString(ACCT_NO_COLUMN_NAME),
                                   result.getString(HOLDER_COLUMN_NAME),
                                   result.getInt(BALANCE_COLUMN_NAME));
            }
            if (!lockExclusive) {
                connection.commit();
            }
        } catch (SQLException sqle) {
            handleException(failureMsg, sqle);
        } finally {
            closeResultSet(failureMsg, result);
        }
        return null;
    }

    public Instrument findInstrumentById(String instrumentId, boolean lockExclusive)
            throws SchoolDBException {
        PreparedStatement stmtToExecute;
        if (lockExclusive) {
            stmtToExecute = findInstrumentByIdStmtLockingForUpdate;
        } else {
            stmtToExecute = findInstrumentByIdStmt;
        }

        String failureMsg = "Could not find instrument!";
        ResultSet result = null;
        System.out.println("log1.1: before execution");
        try {
            stmtToExecute.setString(1, instrumentId);
            System.out.println("log1.2: add parameter");
            System.out.println("log1.3: query: "+stmtToExecute);
            result = stmtToExecute.executeQuery();
            System.out.println("log1.4: execution");
            if (result.next()) {
                System.out.println("log1.5: return result");
                return new Instrument(result.getString(INSTRUMENT_PK_COLUMN_NAME),result.getString(INSTRUMENT_INSTRUMENT_COLUMN_NAME), result.getString(INSTRUMENT_BRAND_COLUMN_NAME), result.getString(INSTRUMENT_CATEGORY_COLUMN_NAME), result.getString("fee"));
            }
            if (!lockExclusive) {
                connection.commit();
            }
        } catch (SQLException sqle) {
            handleException(failureMsg, sqle);
        } finally {
            closeResultSet(failureMsg, result);
        }
        return null;
    }

    public Student findStudentById (String studentId, boolean lockExclusive)
            throws SchoolDBException {
        PreparedStatement stmtToExecute;
        if (lockExclusive) {
            stmtToExecute = findStudentByIdStmtLockingForUpdate;
        } else {
            stmtToExecute = findStudentByIdStmt;
        }

        String failureMsg = "Could not search for specified student.";
        ResultSet result = null;
        try {
            stmtToExecute.setString(1, studentId);
            result = stmtToExecute.executeQuery();
            if (result.next()) {
                return new Student(result.getString(STUDENT_PK_COLUMN_NAME),result.getString(STUDENT_NAME_COLUMN_NAME));
            }
            if (!lockExclusive) {
                connection.commit();
            }
        } catch (SQLException sqle) {
            handleException(failureMsg, sqle);
        } finally {
            closeResultSet(failureMsg, result);
        }
        return null;
    }

    public List<Instrument> findInstrumentsByType (String instrument) throws SchoolDBException {
        String failureMsg = "Could not search for specified instruments.";
        ResultSet result = null;
        List<Instrument> instruments = new ArrayList<>();
        try {
            findInstrumentsByTypeStmt.setString(1, instrument);
            result = findInstrumentsByTypeStmt.executeQuery();
            while (result.next()) {
                instruments.add( new Instrument(result.getString(INSTRUMENT_PK_COLUMN_NAME),result.getString(INSTRUMENT_INSTRUMENT_COLUMN_NAME), result.getString(INSTRUMENT_BRAND_COLUMN_NAME), result.getString(INSTRUMENT_CATEGORY_COLUMN_NAME), result.getString("fee")));
            }
            connection.commit();
        } catch (SQLException sqle) {
            handleException(failureMsg, sqle);
        } finally {
            closeResultSet(failureMsg, result);
        }
        return instruments;
    }

    /**
     * Retrieves all existing accounts.
     *
     * @return A list with all existing accounts. The list is empty if there are no
     *         accounts.
     * @throws SchoolDBException If failed to search for accounts.
     */
    public List<Instrument> findAllInstruments() throws SchoolDBException {
        String failureMsg = "Could not list instruments.";
        List<Instrument> instruments = new ArrayList<>();
        try (ResultSet result = findAllInstrumentsStmt.executeQuery()) {
            while (result.next()) {
                instruments.add( new Instrument(result.getString(INSTRUMENT_PK_COLUMN_NAME),result.getString(INSTRUMENT_INSTRUMENT_COLUMN_NAME), result.getString(INSTRUMENT_BRAND_COLUMN_NAME), result.getString(INSTRUMENT_CATEGORY_COLUMN_NAME), result.getString("fee")));
            }
            connection.commit();
        } catch (SQLException sqle) {
            handleException(failureMsg, sqle);
        }
        return instruments;
    }

    public ArrayList<RentalAgreement> findActiveRentalsForStudent(String studentId, boolean lockExclusive)
            throws SchoolDBException {
        PreparedStatement stmtToExecute;
        if (lockExclusive) {
            stmtToExecute = findActiveRentalsForStudentStmtLockingForUpdate;
        } else {
            stmtToExecute = findActiveRentalsForStudentStmt;
        }

        String failureMsg = "Could not search for specified rentals.";
        ResultSet result = null;
        ArrayList<RentalAgreement> rentals = new ArrayList<>();
        try {
            stmtToExecute.setString(1, studentId);
            result = stmtToExecute.executeQuery();
            if (result.next()) {
                rentals.add(new RentalAgreement(result.getString(INSTRUMENT_PK_COLUMN_NAME), result.getString(STUDENT_PK_COLUMN_NAME), result.getString(RENTAL_AGREEMENT_DATE_RETURNED_COLUMN_NAME), null));
            }
            if (!lockExclusive) {
                connection.commit();
            }
        } catch (SQLException sqle) {
            handleException(failureMsg, sqle);
        } finally {
            closeResultSet(failureMsg, result);
        }
        return null;
    }

    /**
     * Changes the balance of the account with the number of the specified
     * <code>AccountDTO</code> object. The balance is set to the value in the specified
     * <code>AccountDTO</code>.
     *
     * @param account The account to update.
     * @throws SchoolDBException If unable to update the specified account.
     */
    public void updateAccount(AccountDTO account) throws SchoolDBException {
        String failureMsg = "Could not update the account: " + account;
        try {
            changeBalanceStmt.setInt(1, account.getBalance());
            changeBalanceStmt.setString(2, account.getAccountNo());
            int updatedRows = changeBalanceStmt.executeUpdate();
            if (updatedRows != 1) {
                handleException(failureMsg, null);
            }
            connection.commit();
        } catch (SQLException sqle) {
            handleException(failureMsg, sqle);
        }
    }

    public void createRentalAgreement(RentalAgreementDTO rental) throws SchoolDBException {
        String failureMsg = "Could not create the rental!";
        try {
            createRentalAgreementStmt.setString(1, rental.getDateReturned());
            createRentalAgreementStmt.setString(2, rental.getDateReturned());
            createRentalAgreementStmt.setString(3, rental.getStudentId());
            createRentalAgreementStmt.setString(4, rental.getRentalInstrumentId());
            int updatedRows = changeBalanceStmt.executeUpdate();
            if (updatedRows != 1) {
                handleException(failureMsg, null);
            }
            connection.commit();
        } catch (SQLException sqle) {
            handleException(failureMsg, sqle);
        }
    }

    /**
     * Deletes the account with the specified account number.
     *
     * @param acctNo The account to delete.
     * @throws SchoolDBException If unable to delete the specified account.
     */
    public void deleteAccount(String acctNo) throws SchoolDBException {
        String failureMsg = "Could not delete account: " + acctNo;
        try {
            deleteAccountStmt.setString(1, acctNo);
            int updatedRows = deleteAccountStmt.executeUpdate();
            if (updatedRows != 1) {
                handleException(failureMsg, null);
            }
            connection.commit();
        } catch (SQLException sqle) {
            handleException(failureMsg, sqle);
        }
    }

    /**
     * Commits the current transaction.
     * 
     * @throws SchoolDBException If unable to commit the current transaction.
     */
    public void commit() throws SchoolDBException {
        try {
            connection.commit();
        } catch (SQLException e) {
            handleException("Failed to commit", e);
        }
    }

    private void connectToSgmsDB() throws ClassNotFoundException, SQLException {
        connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/sgms",
                                                 "postgres", "postgres");
        connection.setAutoCommit(false);
    }

    private void prepareStatements() throws SQLException {
        createHolderStmt = connection.prepareStatement("INSERT INTO " + HOLDER_TABLE_NAME
            + "(" + HOLDER_COLUMN_NAME + ") VALUES (?)");

        createAccountStmt = connection.prepareStatement("INSERT INTO " + ACCT_TABLE_NAME
            + "(" + ACCT_NO_COLUMN_NAME + ", " + BALANCE_COLUMN_NAME + ", "
            + HOLDER_FK_COLUMN_NAME + ") VALUES (?, ?, ?)");

        findHolderPKStmt = connection.prepareStatement("SELECT " + HOLDER_PK_COLUMN_NAME
            + " FROM " + HOLDER_TABLE_NAME + " WHERE " + HOLDER_COLUMN_NAME + " = ?");

        findAccountByAcctNoStmt = connection.prepareStatement("SELECT a." + ACCT_NO_COLUMN_NAME
            + ", a." + BALANCE_COLUMN_NAME + ", h." + HOLDER_COLUMN_NAME + " from "
            + ACCT_TABLE_NAME + " a INNER JOIN " + HOLDER_TABLE_NAME + " h USING ("
            + HOLDER_PK_COLUMN_NAME + ") WHERE a." + ACCT_NO_COLUMN_NAME + " = ?");

        findAccountByAcctNoStmtLockingForUpdate = connection.prepareStatement("SELECT a." 
            + ACCT_NO_COLUMN_NAME + ", a." + BALANCE_COLUMN_NAME + ", h." 
            + HOLDER_COLUMN_NAME + " from " + ACCT_TABLE_NAME + " a INNER JOIN " 
            + HOLDER_TABLE_NAME + " h USING (" + HOLDER_PK_COLUMN_NAME + ") WHERE a." 
            + ACCT_NO_COLUMN_NAME + " = ? FOR UPDATE");

        findAllInstrumentsStmt = connection.prepareStatement("SELECT ri." + INSTRUMENT_PK_COLUMN_NAME + ", ri." + INSTRUMENT_INSTRUMENT_COLUMN_NAME + ", ri." + INSTRUMENT_BRAND_COLUMN_NAME + ", ri." + INSTRUMENT_CATEGORY_COLUMN_NAME + ", if2."+ INSTRUMENT_FEE_COLUMN_NAME + " FROM " + INSTRUMENT_TABLE_NAME +" ri \n" +
                "FULL JOIN " + RENTAL_AGREEMENT_TABLE_NAME + " ra \n" +
                "ON ra."+ INSTRUMENT_PK_COLUMN_NAME +" = ri."+ INSTRUMENT_PK_COLUMN_NAME +" \n" +
                "FULL JOIN "+ INSTRUMENT_FEE_TABLE_NAME +" if2 \n" +
                "ON ri."+ INSTRUMENT_PK_COLUMN_NAME + " = if2."+ INSTRUMENT_PK_COLUMN_NAME +" \n" +
                "WHERE " + RENTAL_AGREEMENT_DATE_RETURNED_COLUMN_NAME + " IS NOT NULL OR ra." + INSTRUMENT_PK_COLUMN_NAME + " IS NULL");

        findInstrumentsByTypeStmt = connection.prepareStatement("SELECT ri." + INSTRUMENT_PK_COLUMN_NAME + ", ri." + INSTRUMENT_INSTRUMENT_COLUMN_NAME + ", ri." + INSTRUMENT_BRAND_COLUMN_NAME + ", ri." + INSTRUMENT_CATEGORY_COLUMN_NAME + ", if2."+ INSTRUMENT_FEE_COLUMN_NAME + " FROM " + INSTRUMENT_TABLE_NAME +" ri \n" +
                "FULL JOIN " + RENTAL_AGREEMENT_TABLE_NAME + " ra \n" +
                "ON ra."+ INSTRUMENT_PK_COLUMN_NAME +" = ri."+ INSTRUMENT_PK_COLUMN_NAME +" \n" +
                "FULL JOIN "+ INSTRUMENT_FEE_TABLE_NAME +" if2 \n" +
                "ON ri."+ INSTRUMENT_PK_COLUMN_NAME + " = if2."+ INSTRUMENT_PK_COLUMN_NAME +" \n" +
                "WHERE " + RENTAL_AGREEMENT_DATE_RETURNED_COLUMN_NAME + " IS NOT NULL OR ra." + INSTRUMENT_PK_COLUMN_NAME + " IS NULL AND "+ INSTRUMENT_INSTRUMENT_COLUMN_NAME +" = ?");

        changeBalanceStmt = connection.prepareStatement("UPDATE " + ACCT_TABLE_NAME
            + " SET " + BALANCE_COLUMN_NAME + " = ? WHERE " + ACCT_NO_COLUMN_NAME + " = ? ");

        deleteAccountStmt = connection.prepareStatement("DELETE FROM " + ACCT_TABLE_NAME
            + " WHERE " + ACCT_NO_COLUMN_NAME + " = ?");

        findInstrumentByIdStmtLockingForUpdate = connection.prepareStatement("SELECT ri.rental_instrument_id , ri.instrument, ri.brand, if2.fee FROM rental_instrument ri\n" +
                "FULL JOIN instrument_fee if2 \n" +
                "ON ri.rental_instrument_id = if2.rental_instrument_id\n" +
                "WHERE ri.rental_instrument_id = ? FOR UPDATE");

        findInstrumentByIdStmt = connection.prepareStatement("SELECT ri.rental_instrument_id, ri.instrument, ri.brand, if2.fee FROM rental_instrument ri\n" +
                "FULL JOIN instrument_fee if2 \n" +
                "ON ri.rental_instrument_id = if2.rental_instrument_id\n" +
                "WHERE ri.rental_instrument_id = ?");

        findStudentByIdStmtLockingForUpdate = connection.prepareStatement("SELECT student_id, name FROM student s\n" +
                "WHERE student_id = ? FOR UPDATE");

        findStudentByIdStmt = connection.prepareStatement("\"SELECT student_id, name FROM student s\\n\" +\n" +
                "WHERE student_id = ?");

        findActiveRentalsForStudentStmtLockingForUpdate = connection.prepareStatement("SELECT ra.rental_agreement_id, ra.student_id, ra.date_rented  FROM rental_agreement ra \n" +
                "LEFT JOIN student s \n" +
                "ON ra.student_id = s.student_id \n" +
                "WHERE date_returned IS NULL\n" +
                "AND ra.student_id = ? FOR UPDATE");

        findActiveRentalsForStudentStmt = connection.prepareStatement("SELECT ra.rental_agreement_id, ra.student_id, ra.date_rented  FROM rental_agreement ra \n" +
                "LEFT JOIN student s \n" +
                "ON ra.student_id = s.student_id \n" +
                "WHERE date_returned IS NULL\n" +
                "AND ra.student_id = ? FOR UPDATE");

        createRentalAgreementStmt = connection.prepareStatement("INSERT INTO rental_agreement"
                + "(date_rented, date_returned, student_id, rental_instrument_id) VALUES (?, ?, ?, ?)");
    }
    private void handleException(String failureMsg, Exception cause) throws SchoolDBException {
        String completeFailureMsg = failureMsg;
        try {
            connection.rollback();
        } catch (SQLException rollbackExc) {
            completeFailureMsg = completeFailureMsg + 
            ". Also failed to rollback transaction because of: " + rollbackExc.getMessage();
        }

        if (cause != null) {
            throw new SchoolDBException(failureMsg, cause);
        } else {
            throw new SchoolDBException(failureMsg);
        }
    }

    private void closeResultSet(String failureMsg, ResultSet result) throws SchoolDBException {
        try {
            result.close();
        } catch (Exception e) {
            throw new SchoolDBException(failureMsg + " Could not close result set.", e);
        }
    }

    private int createAccountNo() {
        return (int)Math.floor(Math.random() * Integer.MAX_VALUE);
    }

    private int findHolderPKByName(String holderName) throws SQLException {
        ResultSet result = null;
        findHolderPKStmt.setString(1, holderName);
        result = findHolderPKStmt.executeQuery();
        if (result.next()) {
            return result.getInt(HOLDER_PK_COLUMN_NAME);
        }
        return 0;
    }
}
