-- STAGING TABLE

DROP TABLE IF EXISTS staging_healthcare;

CREATE TABLE staging_healthcare (
    name VARCHAR(100),
    age INTEGER,
    gender VARCHAR(20),
    blood_type VARCHAR(10),
    medical_condition VARCHAR(100),
    date_of_admission DATE,
    doctor VARCHAR(100),
    hospital VARCHAR(200),
    insurance_provider VARCHAR(100),
    billing_amount DECIMAL(12,2),
    room_number INTEGER,
    admission_type VARCHAR(50),
    discharge_date DATE,
    medication VARCHAR(100),
    test_results VARCHAR(50)
);


-- VERIFY DATA LOADED

-- How many rows did we import?
SELECT COUNT(*) AS total_rows FROM staging_healthcare;

-- Look at the first 10 rows
SELECT * FROM staging_healthcare LIMIT 10;

-- Check for any NULL values in critical columns
SELECT 
    COUNT(*) AS total_rows,
    COUNT(name) AS rows_with_name,
    COUNT(age) AS rows_with_age,
    COUNT(billing_amount) AS rows_with_billing
FROM staging_healthcare;

-- See the unique values in categorical columns
SELECT DISTINCT medical_condition FROM staging_healthcare ORDER BY 1;
SELECT DISTINCT insurance_provider FROM staging_healthcare ORDER BY 1;
SELECT DISTINCT admission_type FROM staging_healthcare ORDER BY 1;
SELECT DISTINCT test_results FROM staging_healthcare ORDER BY 1;


-- DIMENSION TABLES

DROP TABLE IF EXISTS fact_admissions;
DROP TABLE IF EXISTS dim_patients;
DROP TABLE IF EXISTS dim_doctors;
DROP TABLE IF EXISTS dim_hospitals;
DROP TABLE IF EXISTS dim_insurance;
DROP TABLE IF EXISTS dim_conditions;
DROP TABLE IF EXISTS dim_medications;

-- DIMENSION: Patients
-- Stores unique patient info (name, gender, blood type)
CREATE TABLE dim_patients (
    patient_id SERIAL PRIMARY KEY,  -- Auto-incrementing ID: 1, 2, 3...
    name VARCHAR(100) NOT NULL,
    gender VARCHAR(20),
    blood_type VARCHAR(10)
);

-- DIMENSION: Doctors
CREATE TABLE dim_doctors (
    doctor_id SERIAL PRIMARY KEY,
    doctor_name VARCHAR(100) NOT NULL
);

-- DIMENSION: Hospitals
CREATE TABLE dim_hospitals (
    hospital_id SERIAL PRIMARY KEY,
    hospital_name VARCHAR(200) NOT NULL
);

-- DIMENSION: Insurance Providers
CREATE TABLE dim_insurance (
    insurance_id SERIAL PRIMARY KEY,
    provider_name VARCHAR(100) NOT NULL
);

-- DIMENSION: Medical Conditions
CREATE TABLE dim_conditions (
    condition_id SERIAL PRIMARY KEY,
    condition_name VARCHAR(100) NOT NULL
);

-- DIMENSION: Medications
CREATE TABLE dim_medications (
    medication_id SERIAL PRIMARY KEY,
    medication_name VARCHAR(100) NOT NULL
);


-- FACT TABLE

CREATE TABLE fact_admissions (
    admission_id SERIAL PRIMARY KEY,
    
    -- Foreign keys
    patient_id INTEGER REFERENCES dim_patients(patient_id),
    doctor_id INTEGER REFERENCES dim_doctors(doctor_id),
    hospital_id INTEGER REFERENCES dim_hospitals(hospital_id),
    insurance_id INTEGER REFERENCES dim_insurance(insurance_id),
    condition_id INTEGER REFERENCES dim_conditions(condition_id),
    medication_id INTEGER REFERENCES dim_medications(medication_id),
    
    -- Measures
    patient_age INTEGER,
    admission_date DATE,
    discharge_date DATE,
    admission_type VARCHAR(50),
    room_number INTEGER,
    billing_amount DECIMAL(12,2),
    test_results VARCHAR(50),
    
    -- Calculated column:
    length_of_stay INTEGER GENERATED ALWAYS AS (discharge_date - admission_date) STORED
);

-- Indexes for faster queries
CREATE INDEX idx_fact_admission_date ON fact_admissions(admission_date);
CREATE INDEX idx_fact_hospital ON fact_admissions(hospital_id);
CREATE INDEX idx_fact_condition ON fact_admissions(condition_id);
CREATE INDEX idx_fact_insurance ON fact_admissions(insurance_id);


-- POPULATE DIMENSION TABLES

-- Insert unique patients
INSERT INTO dim_patients (name, gender, blood_type)
SELECT DISTINCT 
    name, 
    gender, 
    blood_type
FROM staging_healthcare
WHERE name IS NOT NULL;

-- Insert unique doctors
INSERT INTO dim_doctors (doctor_name)
SELECT DISTINCT doctor
FROM staging_healthcare
WHERE doctor IS NOT NULL;

-- Insert unique hospitals
INSERT INTO dim_hospitals (hospital_name)
SELECT DISTINCT hospital
FROM staging_healthcare
WHERE hospital IS NOT NULL;

-- Insert unique insurance providers
INSERT INTO dim_insurance (provider_name)
SELECT DISTINCT insurance_provider
FROM staging_healthcare
WHERE insurance_provider IS NOT NULL;

-- Insert unique medical conditions
INSERT INTO dim_conditions (condition_name)
SELECT DISTINCT medical_condition
FROM staging_healthcare
WHERE medical_condition IS NOT NULL;

-- Insert unique medications
INSERT INTO dim_medications (medication_name)
SELECT DISTINCT medication
FROM staging_healthcare
WHERE medication IS NOT NULL;


-- VERIFY DIM TABLES

SELECT 'dim_patients' AS table_name, COUNT(*) AS row_count FROM dim_patients
UNION ALL
SELECT 'dim_doctors', COUNT(*) FROM dim_doctors
UNION ALL
SELECT 'dim_hospitals', COUNT(*) FROM dim_hospitals
UNION ALL
SELECT 'dim_insurance', COUNT(*) FROM dim_insurance
UNION ALL
SELECT 'dim_conditions', COUNT(*) FROM dim_conditions
UNION ALL
SELECT 'dim_medications', COUNT(*) FROM dim_medications;


-- POPULATE FACT TABLE

INSERT INTO fact_admissions (
    patient_id,
    doctor_id,
    hospital_id,
    insurance_id,
    condition_id,
    medication_id,
    patient_age,
    admission_date,
    discharge_date,
    admission_type,
    room_number,
    billing_amount,
    test_results
)
SELECT 
    p.patient_id,
    d.doctor_id,
    h.hospital_id,
    i.insurance_id,
    c.condition_id,
    m.medication_id,
    s.age,
    s.date_of_admission,
    s.discharge_date,
    s.admission_type,
    s.room_number,
    s.billing_amount,
    s.test_results
FROM staging_healthcare s
JOIN dim_patients p ON s.name = p.name 
                    AND s.gender = p.gender 
                    AND s.blood_type = p.blood_type
JOIN dim_doctors d ON s.doctor = d.doctor_name
JOIN dim_hospitals h ON s.hospital = h.hospital_name
JOIN dim_insurance i ON s.insurance_provider = i.provider_name
JOIN dim_conditions c ON s.medical_condition = c.condition_name
JOIN dim_medications m ON s.medication = m.medication_name;


-- VERIFY

SELECT COUNT(*) AS fact_table_rows FROM fact_admissions;

-- Preview the data
SELECT 
    f.admission_id,
    p.name AS patient_name,
    p.gender,
    f.patient_age,
    h.hospital_name,
    d.doctor_name,
    c.condition_name,
    m.medication_name,
    i.provider_name AS insurance,
    f.admission_date,
    f.discharge_date,
    f.length_of_stay,
    f.billing_amount,
    f.test_results
FROM fact_admissions f
JOIN dim_patients p ON f.patient_id = p.patient_id
JOIN dim_doctors d ON f.doctor_id = d.doctor_id
JOIN dim_hospitals h ON f.hospital_id = h.hospital_id
JOIN dim_insurance i ON f.insurance_id = i.insurance_id
JOIN dim_conditions c ON f.condition_id = c.condition_id
JOIN dim_medications m ON f.medication_id = m.medication_id
LIMIT 20;


-- Summary Stats

SELECT
    COUNT(*) AS total_admissions,
    COUNT(DISTINCT patient_id) AS unique_patients,
    COUNT(DISTINCT hospital_id) AS hospitals,
    COUNT(DISTINCT doctor_id) AS doctors,
    ROUND(AVG(billing_amount), 2) AS avg_billing,
    ROUND(AVG(length_of_stay), 1) AS avg_length_of_stay,
    MIN(admission_date) AS earliest_admission,
    MAX(admission_date) AS latest_admission
FROM fact_admissions;


-- QUESTIONS:


-- Q1: What does our patient population look like by age group?

-- Admissions by age group: what percent of admissions come from each age group?
SELECT
    CASE 
        WHEN patient_age BETWEEN 0 AND 17 THEN 'Pediatric (0-17)'
        WHEN patient_age BETWEEN 18 AND 34 THEN 'Young Adult (18-34)'
        WHEN patient_age BETWEEN 35 AND 49 THEN 'Middle Age (35-49)'
        WHEN patient_age BETWEEN 50 AND 65 THEN 'Older Adult (50-65)'
        WHEN patient_age > 65 THEN 'Senior (66+)'
    END AS age_group,
    COUNT(*) AS admission_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM fact_admissions
GROUP BY age_group
ORDER BY admission_count DESC;

-- Unique patients by age group(using patient age at first visit): what percentage of unique patients fall into each group?
WITH first_visit AS (
    SELECT 
        patient_id,
        patient_age,
        ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY admission_date) AS visit_num
    FROM fact_admissions
)
SELECT
    CASE 
        WHEN patient_age BETWEEN 0 AND 17 THEN 'Pediatric (0-17)'
        WHEN patient_age BETWEEN 18 AND 34 THEN 'Young Adult (18-34)'
        WHEN patient_age BETWEEN 35 AND 49 THEN 'Middle Age (35-49)'
        WHEN patient_age BETWEEN 50 AND 65 THEN 'Older Adult (50-65)'
        WHEN patient_age > 65 THEN 'Senior (66+)'
    END AS age_group,
    COUNT(*) AS patient_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM first_visit
WHERE visit_num = 1
GROUP BY age_group
ORDER BY patient_count DESC;


-- Q2. What is the overall blood type distribution, and does it vary by gender?

SELECT 
    p.blood_type,
    COUNT(DISTINCT f.patient_id) FILTER (WHERE p.gender = 'Male') AS male_count,
    COUNT(DISTINCT f.patient_id) FILTER (WHERE p.gender = 'Female') AS female_count,
    ROUND(
        COUNT(DISTINCT f.patient_id) FILTER (WHERE p.gender = 'Male') * 100.0 /
        SUM(COUNT(DISTINCT f.patient_id) FILTER (WHERE p.gender = 'Male')) OVER (),
    2) AS male_pct,
    ROUND(
        COUNT(DISTINCT f.patient_id) FILTER (WHERE p.gender = 'Female') * 100.0 /
        SUM(COUNT(DISTINCT f.patient_id) FILTER (WHERE p.gender = 'Female')) OVER (),
    2) AS female_pct
FROM fact_admissions f
JOIN dim_patients p ON f.patient_id = p.patient_id
GROUP BY p.blood_type
ORDER BY p.blood_type;


-- Q3. How does average billing vary by medical condition?

SELECT
	c.condition_name,
	COUNT(*) AS admission_count,
	ROUND(AVG(f.billing_amount), 2) AS avg_billing_amount,
	ROUND(MIN(f.billing_amount), 2) AS min_billing_amount,
	ROUND(MAX(f.billing_amount), 2) AS max_billing_amount,
	ROUND(SUM(f.billing_amount), 2) AS total_billing_amount
FROM fact_admissions f
JOIN dim_conditions c ON f.condition_id = c.condition_id
GROUP BY c.condition_name
ORDER BY avg_billing_amount DESC;


-- Q4. Which insurance provider pays the most on average, and does this vary by condition?

SELECT
	c.condition_name,
	i.provider_name,
	COUNT(*) AS admission_count,
	ROUND(AVG(f.billing_amount), 2) AS avg_billing_amount,
	RANK() OVER (PARTITION BY c.condition_name ORDER BY AVG(f.billing_amount) DESC) AS rank_within_condition
FROM fact_admissions f
JOIN dim_conditions c ON f.condition_id = c.condition_id
JOIN dim_insurance i ON f.insurance_id = i.insurance_id
GROUP BY 1, 2
ORDER BY 1, avg_billing_amount DESC;


-- Q5. What's the monthly revenue trend over the 5-year period?

SELECT
    EXTRACT(YEAR FROM admission_date) AS year,
    EXTRACT(MONTH FROM admission_date) AS month_num,
    TRIM(TO_CHAR(admission_date, 'Month')) AS month_name,
    COUNT(*) AS admission_count,
    ROUND(SUM(billing_amount), 2) AS total_revenue,
    ROUND(AVG(billing_amount), 2) AS avg_revenue_per_admission,
    LAG(ROUND(SUM(billing_amount), 2)) OVER (
        PARTITION BY EXTRACT(MONTH FROM admission_date) 
        ORDER BY EXTRACT(YEAR FROM admission_date)
    ) AS same_month_last_year,
    ROUND(
        (SUM(billing_amount) - LAG(SUM(billing_amount)) OVER (
            PARTITION BY EXTRACT(MONTH FROM admission_date) 
            ORDER BY EXTRACT(YEAR FROM admission_date)
        )) * 100.0 /
        NULLIF(LAG(SUM(billing_amount)) OVER (
            PARTITION BY EXTRACT(MONTH FROM admission_date) 
            ORDER BY EXTRACT(YEAR FROM admission_date)
        ), 0),
    2) AS yoy_pct_change
FROM fact_admissions
GROUP BY 1, 2, 3
ORDER BY 1, 2;


-- Q6. Are there billing outliers, and which conditions have the most variance in cost?

-- P1. Which conditions have the the most variance in cost?
SELECT
    c.condition_name,
    COUNT(*) AS admission_count,
    ROUND(AVG(f.billing_amount), 2) AS avg_billing,
    ROUND(MIN(f.billing_amount), 2) AS min_billing,
    ROUND(MAX(f.billing_amount), 2) AS max_billing,
    ROUND(MAX(f.billing_amount) - MIN(f.billing_amount), 2) AS billing_range,
    ROUND(STDDEV(f.billing_amount), 2) AS std_dev,
    ROUND(STDDEV(f.billing_amount) / AVG(f.billing_amount) * 100, 2) AS coefficient_of_variation
FROM fact_admissions f
JOIN dim_conditions c ON f.condition_id = c.condition_id
GROUP BY c.condition_name
ORDER BY std_dev DESC;

-- P2. Flag individual outlier bills

WITH billing_stats AS (
    SELECT
        f.admission_id,
        f.patient_id,
        f.billing_amount,
        f.admission_date,
        c.condition_name,
        AVG(f.billing_amount) OVER (PARTITION BY f.condition_id) AS condition_avg,
        STDDEV(f.billing_amount) OVER (PARTITION BY f.condition_id) AS condition_stddev
    FROM fact_admissions f
    JOIN dim_conditions c ON f.condition_id = c.condition_id
)
SELECT
    admission_id,
    patient_id,
    condition_name,
    admission_date,
    ROUND(billing_amount, 2) AS billing_amount,
    ROUND(condition_avg, 2) AS condition_avg,
    ROUND((billing_amount - condition_avg) / NULLIF(condition_stddev, 0), 2) AS z_score,
    CASE 
        WHEN (billing_amount - condition_avg) / NULLIF(condition_stddev, 0) > 2 THEN 'High Outlier'
        WHEN (billing_amount - condition_avg) / NULLIF(condition_stddev, 0) < -2 THEN 'Low Outlier'
        ELSE 'Normal'
    END AS outlier_flag
FROM billing_stats
WHERE ABS((billing_amount - condition_avg) / NULLIF(condition_stddev, 0)) > 2
ORDER BY ABS((billing_amount - condition_avg) / NULLIF(condition_stddev, 0)) DESC;

-- P3. Summary count of outlier by condition

WITH billing_stats AS (
    SELECT
        f.admission_id,
        f.condition_id,
        f.billing_amount,
        AVG(f.billing_amount) OVER (PARTITION BY f.condition_id) AS condition_avg,
        STDDEV(f.billing_amount) OVER (PARTITION BY f.condition_id) AS condition_stddev
    FROM fact_admissions f
),
flagged AS (
    SELECT
        condition_id,
        CASE 
            WHEN (billing_amount - condition_avg) / NULLIF(condition_stddev, 0) > 2 THEN 'High Outlier'
            WHEN (billing_amount - condition_avg) / NULLIF(condition_stddev, 0) < -2 THEN 'Low Outlier'
            ELSE 'Normal'
        END AS outlier_flag
    FROM billing_stats
)
SELECT
    c.condition_name,
    COUNT(*) AS total_admissions,
    SUM(CASE WHEN outlier_flag = 'High Outlier' THEN 1 ELSE 0 END) AS high_outliers,
    SUM(CASE WHEN outlier_flag = 'Low Outlier' THEN 1 ELSE 0 END) AS low_outliers,
    ROUND(SUM(CASE WHEN outlier_flag != 'Normal' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS outlier_pct
FROM flagged f
JOIN dim_conditions c ON f.condition_id = c.condition_id
GROUP BY c.condition_name
ORDER BY outlier_pct DESC;


-- Clinical Outcomes

-- Q7. For each medical condition, which medication is most commonly prescribed and which has the best outcomes?

SELECT 
    c.condition_name,
    m.medication_name,
    COUNT(*) AS prescription_count,
    SUM(CASE WHEN f.test_results = 'Normal' THEN 1 ELSE 0 END) AS normal_outcomes,
    SUM(CASE WHEN f.test_results = 'Abnormal' THEN 1 ELSE 0 END) AS abnormal_outcomes,
    SUM(CASE WHEN f.test_results = 'Inconclusive' THEN 1 ELSE 0 END) AS inconclusive_outcomes,
    ROUND(
        SUM(CASE WHEN f.test_results = 'Normal' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
    2) AS normal_outcome_pct,
    RANK() OVER (PARTITION BY c.condition_name ORDER BY COUNT(*) DESC) AS most_prescribed_rank,
    RANK() OVER (PARTITION BY c.condition_name ORDER BY SUM(CASE WHEN f.test_results = 'Normal' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) DESC) AS best_outcome_rank
FROM fact_admissions f
JOIN dim_conditions c ON f.condition_id = c.condition_id
JOIN dim_medications m ON f.medication_id = m.medication_id
GROUP BY c.condition_name, m.medication_name
ORDER BY c.condition_name, prescription_count DESC;

-- Q8. How does length of stay vary by medical condition and admission type?

SELECT
    c.condition_name,
    f.admission_type,
    COUNT(*) AS admission_count,
	ROUND(AVG(f.length_of_stay), 1) AS avg_los,
	MIN(f.length_of_stay) AS min_los,
	MAX(f.length_of_stay) AS max_los
FROM fact_admissions f
JOIN dim_conditions c ON f.condition_id = c.condition_id
GROUP BY c.condition_name, f.admission_type
ORDER BY c.condition_name, f.admission_type;

-- Q9. What percentage of admissions are Emergency vs Elective vs Urgent, and how does this differ by age group?

WITH age_grouped AS (
    SELECT
        CASE 
            WHEN patient_age BETWEEN 0 AND 17 THEN 'Pediatric (0-17)'
            WHEN patient_age BETWEEN 18 AND 34 THEN 'Young Adult (18-34)'
            WHEN patient_age BETWEEN 35 AND 49 THEN 'Middle Age (35-49)'
            WHEN patient_age BETWEEN 50 AND 65 THEN 'Older Adult (50-65)'
            WHEN patient_age > 65 THEN 'Senior (66+)'
        END AS age_group,
        admission_type
    FROM fact_admissions
)
SELECT
    age_group,
    admission_type,
    COUNT(*) AS admission_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY age_group), 2) AS pct_of_admissions
FROM age_grouped
GROUP BY age_group, admission_type
ORDER BY age_group, admission_count DESC;


-- Trends & Patterns

-- Q10. Are there seasonal patterns in admissions (monthly/quarterly)?

SELECT
	EXTRACT(MONTH FROM admission_date) AS month_num,
	TRIM(TO_CHAR(admission_date, 'Month')) AS month_name,
	COUNT(*) AS admission_count,
	ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS busiest_rank
FROM fact_admissions
GROUP BY 1, 2
ORDER BY month_num

-- Q11. Do emergency admissions have different outcomes than elective ones?

SELECT
    admission_type,
    COUNT(*) AS total_admissions,
    SUM(CASE WHEN test_results = 'Normal' THEN 1 ELSE 0 END) AS normal_count,
    SUM(CASE WHEN test_results = 'Abnormal' THEN 1 ELSE 0 END) AS abnormal_count,
    SUM(CASE WHEN test_results = 'Inconclusive' THEN 1 ELSE 0 END) AS inconclusive_count,
    ROUND(SUM(CASE WHEN test_results = 'Normal' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS normal_pct,
    ROUND(SUM(CASE WHEN test_results = 'Abnormal' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS abnormal_pct,
    ROUND(SUM(CASE WHEN test_results = 'Inconclusive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS inconclusive_pct,
    ROUND(AVG(length_of_stay), 1) AS avg_los,
    ROUND(AVG(billing_amount), 2) AS avg_billing
FROM fact_admissions
GROUP BY admission_type
ORDER BY admission_type;

-- Q12. Which patients are repeat visitors, and what conditions bring them back?

-- P1. Who are the repeat visitors?
SELECT
    p.patient_id,
    p.name,
    p.gender,
    COUNT(*) AS visit_count
FROM fact_admissions f
JOIN dim_patients p ON f.patient_id = p.patient_id
GROUP BY p.patient_id, p.name, p.gender
HAVING COUNT(*) > 1
ORDER BY visit_count DESC;

-- P2. How many repeat visitors vs one-time visitors?

WITH visit_counts AS (
    SELECT
        patient_id,
        COUNT(*) AS visit_count
    FROM fact_admissions
    GROUP BY patient_id
)
SELECT
    CASE 
        WHEN visit_count = 1 THEN 'One-time'
        WHEN visit_count = 2 THEN '2 visits'
        WHEN visit_count >= 3 THEN '3+ visits'
    END AS visitor_type,
    COUNT(*) AS patient_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_patients
FROM visit_counts
GROUP BY visitor_type
ORDER BY patient_count DESC;

-- P3. Which conditions have the highest repeat visit rate?

WITH patient_visits AS (
    SELECT
        patient_id,
        COUNT(*) AS total_visits
    FROM fact_admissions
    GROUP BY patient_id
),
repeat_patients AS (
    SELECT patient_id
    FROM patient_visits
    WHERE total_visits > 1
)
SELECT
    c.condition_name,
    COUNT(*) AS total_admissions,
    COUNT(DISTINCT f.patient_id) AS unique_patients,
    COUNT(DISTINCT CASE WHEN rp.patient_id IS NOT NULL THEN f.patient_id END) AS repeat_patients,
    ROUND(
        COUNT(DISTINCT CASE WHEN rp.patient_id IS NOT NULL THEN f.patient_id END) * 100.0 / 
        COUNT(DISTINCT f.patient_id), 
    2) AS repeat_patient_pct
FROM fact_admissions f
JOIN dim_conditions c ON f.condition_id = c.condition_id
LEFT JOIN repeat_patients rp ON f.patient_id = rp.patient_id
GROUP BY c.condition_name
ORDER BY repeat_patient_pct DESC;


-- Tableau Export Views

CREATE OR REPLACE VIEW vw_admissions_master AS
SELECT
    f.admission_id,
    f.patient_id,
    p.name AS patient_name,
    p.gender,
    p.blood_type,
    f.patient_age,
    CASE 
        WHEN f.patient_age BETWEEN 0 AND 17 THEN 'Pediatric (0-17)'
        WHEN f.patient_age BETWEEN 18 AND 34 THEN 'Young Adult (18-34)'
        WHEN f.patient_age BETWEEN 35 AND 49 THEN 'Middle Age (35-49)'
        WHEN f.patient_age BETWEEN 50 AND 65 THEN 'Older Adult (50-65)'
        WHEN f.patient_age > 65 THEN 'Senior (66+)'
    END AS age_group,
    c.condition_name,
    m.medication_name,
    i.provider_name AS insurance_provider,
    f.admission_type,
    f.admission_date,
    f.discharge_date,
    f.length_of_stay,
    f.room_number,
    f.billing_amount,
    f.test_results,
    EXTRACT(YEAR FROM f.admission_date) AS admission_year,
    EXTRACT(MONTH FROM f.admission_date) AS admission_month,
    TRIM(TO_CHAR(f.admission_date, 'Month')) AS admission_month_name,
    EXTRACT(QUARTER FROM f.admission_date) AS admission_quarter
FROM fact_admissions f
JOIN dim_patients p ON f.patient_id = p.patient_id
JOIN dim_conditions c ON f.condition_id = c.condition_id
JOIN dim_medications m ON f.medication_id = m.medication_id
JOIN dim_insurance i ON f.insurance_id = i.insurance_id;

SELECT * FROM vw_admissions_master;

