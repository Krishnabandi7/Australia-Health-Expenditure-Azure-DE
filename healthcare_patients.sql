-- Healthcare Patients Table
-- This table stores patient information for healthcare analysis
-- Created: 2026-05-23

CREATE TABLE IF NOT EXISTS healthcare_patients (
    patient_id INT PRIMARY KEY IDENTITY(1,1),
    first_name NVARCHAR(100) NOT NULL,
    last_name NVARCHAR(100) NOT NULL,
    date_of_birth DATE NOT NULL,
    gender CHAR(1) NOT NULL,
    phone_number NVARCHAR(20),
    email NVARCHAR(100),
    address NVARCHAR(255),
    city NVARCHAR(100),
    state NVARCHAR(50),
    postal_code NVARCHAR(10),
    country NVARCHAR(100),
    insurance_provider NVARCHAR(100),
    insurance_policy_number NVARCHAR(100),
    medical_conditions NVARCHAR(MAX),
    current_medications NVARCHAR(MAX),
    allergies NVARCHAR(MAX),
    emergency_contact_name NVARCHAR(100),
    emergency_contact_phone NVARCHAR(20),
    created_date DATETIME DEFAULT GETDATE(),
    modified_date DATETIME DEFAULT GETDATE(),
    is_active BIT DEFAULT 1
);

-- Create index on commonly searched fields
CREATE INDEX IDX_Patient_Name ON healthcare_patients(last_name, first_name);
CREATE INDEX IDX_Patient_Insurance ON healthcare_patients(insurance_provider);
CREATE INDEX IDX_Patient_Active ON healthcare_patients(is_active);
