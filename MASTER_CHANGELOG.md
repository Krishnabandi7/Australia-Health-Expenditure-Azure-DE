# Master Changelog - Australia Health Expenditure Azure DE

All notable changes to this project will be documented in this file.
**Primary Version Control: `db-changelog.xml`**

## Database Schema Versions

### v1.0.0 - Initial Database Schema (2026-05-23)

#### New Tables
- **healthcare_patients**: Patient demographic and medical information storage
  - Records: Patient information including personal details, insurance, and medical history
  - Indexes: Name, Insurance Provider, Active status
  - Purpose: Central repository for patient data in healthcare analysis pipeline

#### SQL Files Created
- `healthcare_patients.sql`: Schema definition for patients table

#### Version Control Files
- **`db-changelog.xml`** - Liquibase/Flyway compatible changelog with version tracking
  - ChangeSet ID: `1.0.0-create-healthcare-patients-table`
  - References: `healthcare_patients.sql`
  - Includes automatic rollback instructions

#### Documentation
- `CHANGELOG.md`: Detailed changelog for schema changes

---

## File Structure
```
Australia-Health-Expenditure-Azure-DE/
├── db-changelog.xml                 # Master version control (XML format)
├── MASTER_CHANGELOG.md              # This file
├── CHANGELOG.md                     # Detailed changes
├── healthcare_patients.sql          # SQL schema definition
├── README.md                        # Project overview
├── Databricks/                      # Databricks notebooks
├── Raw_data/                        # Raw healthcare data
└── au_data-engineering.pbix        # Power BI dashboard
```

## Maintenance Notes
- All database changes must be tracked in `db-changelog.xml`
- Update `db-changelog.xml` with new changeSet entries for each schema modification
- Ensure created_date and modified_date are maintained during insert/update operations
- Regular backups recommended before schema modifications
- Index maintenance should be scheduled periodically for optimal query performance
