# Healthcare Analytics Project: Data Analysis Using SQL

![Hospital]()

## Overview

This project involves a comprehensive analysis of healthcare patient data using PostgreSQL. The goal is to extract actionable business insights through SQL queries covering patient demographics, financial performance, clinical outcomes, and operational efficiency.

This README documents the objectives, dataset, schema, business problems, SQL solutions, and key findings.

## Objectives

- Analyze patient demographics and population distribution across age groups and genders
- Evaluate financial performance by medical condition, insurance provider, and time period
- Understand clinical outcomes and treatment effectiveness across conditions and medications
- Assess operational metrics including length of stay and admission patterns
- Identify billing outliers and cost variance for compliance review

## Dataset

The dataset represents synthetic healthcare patient records with information on admissions, diagnoses, treatments, billing, and outcomes over a 5-year period (2019-2024).

**Dataset Link:** [Healthcare Dataset | Kaggle](https://www.kaggle.com/datasets/prasad22/healthcare-dataset)

**Dataset Statistics:**
| Metric | Value |
|--------|-------|
| Total Admissions | 55,500 |
| Unique Patients | 49,999 |
| Date Range | May 2019 - May 2024 |
| Medical Conditions | 6 |
| Insurance Providers | 5 |
| Medications | 5 |

## Schema

### Staging Table
```sql
-- STAGING TABLE: Temporary landing zone for raw CSV data
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
```

### Dimension Tables
```sql
-- DIMENSION: Patients
CREATE TABLE dim_patients (
    patient_id SERIAL PRIMARY KEY,
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
```

### Fact Table
```sql
-- FACT TABLE: Patient Admissions
CREATE TABLE fact_admissions (
    admission_id SERIAL PRIMARY KEY,
    patient_id INTEGER REFERENCES dim_patients(patient_id),
    doctor_id INTEGER REFERENCES dim_doctors(doctor_id),
    hospital_id INTEGER REFERENCES dim_hospitals(hospital_id),
    insurance_id INTEGER REFERENCES dim_insurance(insurance_id),
    condition_id INTEGER REFERENCES dim_conditions(condition_id),
    medication_id INTEGER REFERENCES dim_medications(medication_id),
    patient_age INTEGER,
    admission_date DATE,
    discharge_date DATE,
    admission_type VARCHAR(50),
    room_number INTEGER,
    billing_amount DECIMAL(12,2),
    test_results VARCHAR(50),
    length_of_stay INTEGER GENERATED ALWAYS AS (discharge_date - admission_date) STORED
);

-- Indexes for query performance
CREATE INDEX idx_fact_admission_date ON fact_admissions(admission_date);
CREATE INDEX idx_fact_hospital ON fact_admissions(hospital_id);
CREATE INDEX idx_fact_condition ON fact_admissions(condition_id);
CREATE INDEX idx_fact_insurance ON fact_admissions(insurance_id);
```

## Business Problems and Solutions

### Patient Demographics

#### Q1. What does our patient population look like by age group?

**Objective:** Understand patient demographics to inform staffing decisions, resource allocation, and targeted care programs.

```sql
-- Unique patients by age group (using patient age at first visit)
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
```

#### Q2. What is the overall blood type distribution, and does it vary by gender?

**Objective:** Support blood bank inventory planning and emergency preparedness across facilities.

```sql
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
```

---

### Financial Analysis

#### Q3. How does average billing vary by medical condition?

**Objective:** Understand cost drivers by condition to inform pricing strategies and resource allocation.

```sql
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
```

#### Q4. Which insurance provider pays the most on average, and does this vary by condition?

**Objective:** Support insurance contract negotiations by identifying high and low-paying payers by condition.

```sql
SELECT
    c.condition_name,
    i.provider_name,
    COUNT(*) AS admission_count,
    ROUND(AVG(f.billing_amount), 2) AS avg_billing_amount,
    RANK() OVER (PARTITION BY c.condition_name ORDER BY AVG(f.billing_amount) DESC) AS rank_within_condition
FROM fact_admissions f
JOIN dim_conditions c ON f.condition_id = c.condition_id
JOIN dim_insurance i ON f.insurance_id = i.insurance_id
GROUP BY c.condition_name, i.provider_name
ORDER BY c.condition_name, avg_billing_amount DESC;
```

#### Q5. What's the monthly revenue trend over the 5-year period?

**Objective:** Track revenue performance over time and identify year-over-year growth patterns.

```sql
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
```

#### Q6. Are there billing outliers, and which conditions have the most variance in cost?

**Objective:** Identify billing errors, potential fraud, and pricing inconsistencies for compliance review.

```sql
-- Part 1: Which conditions have the most variance in cost?
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

-- Part 2: Flag individual outlier bills using Z-score
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

-- Part 3: Summary count of outliers by condition
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
```

---

### Clinical Outcomes

#### Q7. For each medical condition, which medication is most commonly prescribed and which has the best outcomes?

**Objective:** Evaluate treatment protocols and identify opportunities for formulary optimization.

```sql
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
```

#### Q8. How does length of stay vary by medical condition and admission type?

**Objective:** Support capacity planning and discharge optimization by understanding stay patterns.

```sql
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
```

#### Q9. What percentage of admissions are Emergency vs Elective vs Urgent, and how does this differ by age group?

**Objective:** Understand admission patterns by age to optimize ER staffing and surgical scheduling.

```sql
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
```

---

### Trends & Patterns

#### Q10. Are there seasonal patterns in admissions (monthly/quarterly)?

**Objective:** Anticipate demand fluctuations for capacity planning and staffing optimization.

```sql
SELECT
    EXTRACT(MONTH FROM admission_date) AS month_num,
    TRIM(TO_CHAR(admission_date, 'Month')) AS month_name,
    COUNT(*) AS admission_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS busiest_rank
FROM fact_admissions
GROUP BY 1, 2
ORDER BY month_num;
```

#### Q11. Do emergency admissions have different outcomes than elective ones?

**Objective:** Compare quality metrics across admission types to set expectations and identify improvement areas.

```sql
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
```

#### Q12. Which patients are repeat visitors, and what conditions bring them back?

**Objective:** Identify readmission patterns to target quality improvement and care coordination efforts.

```sql
-- Part 1: Who are the repeat visitors?
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

-- Part 2: How many repeat visitors vs one-time visitors?
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

-- Part 3: Which conditions have the highest repeat visit rate?
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
```

---

## Findings and Conclusion

### Key Insights

1. **Patient Demographics:** Patient population is evenly distributed across age groups, with each segment representing approximately 20% of unique patients. Blood type distribution is consistent across genders.

2. **Financial Performance:** Total billing across the 5-year period exceeded $1.4 billion. Billing is relatively consistent across medical conditions, with an average of ~$25,500 per admission.

3. **Clinical Outcomes:** Outcome rates (Normal/Abnormal/Inconclusive) are evenly distributed across admission types, suggesting admission urgency does not significantly impact outcomes in this dataset.

4. **Operational Metrics:** Average length of stay is approximately 15.5 days across all conditions. No significant variation exists between emergency, elective, and urgent admissions.

5. **Repeat Visitors:** Approximately 10% of patients have multiple admissions. Most repeat patients return for the same condition, indicating potential chronic disease management opportunities.

6. **Data Characteristics:** The synthetic nature of the dataset results in even distributions across most dimensions. Real healthcare data would likely show more variance in billing, outcomes, and length of stay.

### Technical Skills Demonstrated

- **SQL:** CTEs, Window Functions, CASE statements, JOINs, Aggregations, Statistical Analysis (Z-scores, Standard Deviation)
- **Database Design:** Star Schema, Dimension/Fact Tables, Indexing, Views

---

## Author

**Shivani**

This project is part of my data analytics portfolio, demonstrating SQL skills applied to healthcare data analysis.

- LinkedIn: [www.linkedin.com/in/shivanidesai111]
- GitHub: [https://github.com/shivanidesai30]

---

## Acknowledgments

- Dataset: [Prasad22 on Kaggle](https://www.kaggle.com/datasets/prasad22/healthcare-dataset)
- Tools: PostgreSQL, pgAdmin, Tableau

---

Thank you for reviewing this project!
