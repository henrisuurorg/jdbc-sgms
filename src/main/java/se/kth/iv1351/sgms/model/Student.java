package se.kth.iv1351.sgms.model;

import java.util.ArrayList;

public class Student {
    private String studentId;
    private String name;
    private ArrayList<RentalAgreementDTO> activeAgreements;

    public Student(String studentId, String name) {
        this.studentId = studentId;
        this.name = name;
    }

    public RentalAgreement rent(String rentalInstrumentId) throws RejectedException {
        if (activeAgreements.size() >= 2) {
            throw new RejectedException("Tried to rent more than 2 instruments for student: " + studentId);
        }
        return new RentalAgreement(rentalInstrumentId, studentId, "12/14/2022" ,null);
    }

    public void addRentals(ArrayList<RentalAgreement> rentals){
        activeAgreements.addAll(rentals);
    }
}
