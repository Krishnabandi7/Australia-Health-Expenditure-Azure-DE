# Changelog - Healthcare Patients Table

## [1.0.0] - 2026-05-23

### Added
- Created new `healthcare_patients` table to store patient information
- Fields include: patient_id, first_name, last_name, date_of_birth, gender, contact information
- Added insurance provider and policy tracking
- Added medical history fields: medical_conditions, current_medications, allergies
- Added emergency contact information
- Added audit columns: created_date, modified_date, is_active
- Created indexes on frequently queried columns:
  - IDX_Patient_Name (last_name, first_name)
  - IDX_Patient_Insurance (insurance_provider)
  - IDX_Patient_Active (is_active)

### Details
- Table uses IDENTITY column for auto-incrementing patient_id
- Default timezone-aware timestamps with GETDATE()
- All patients default to active status (is_active = 1)
- Supports storing multiple medical conditions, medications, and allergies as text fields
