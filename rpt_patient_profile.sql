CREATE VIEW [rpt].[vw_patient_profile] AS

/****************************************************************************************
CTE: visits
Purpose: Gathers the last 36 months of completed appointments per patient,
         capturing relevant visit context and provider details. The latest visit is
         marked using ROW_NUMBER.
****************************************************************************************/
WITH visits AS (
    SELECT
        mrn,
        csn AS pat_enc_csn_id,
        fin_class_name,
        appt_group,
        appt_status,
        appt_date,
        age,
        department_name,
        dept_specialty_name,
        center_name,
        visit_prov_id,
        visit_provider_name,
        visit_provider_type,
        ROW_NUMBER() OVER (PARTITION BY mrn ORDER BY appt_date DESC) AS rn
    FROM rpt.appointment
    WHERE appt_status = 'Completed'
      AND appt_date >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 36, 0)
),

/****************************************************************************************
CTE: visit_scheduled
Purpose: Identifies upcoming scheduled appointments to determine future patient engagement.
****************************************************************************************/
visit_scheduled AS (
    SELECT
        mrn,
        appt_date AS next_scheduled_appt,
        ROW_NUMBER() OVER (PARTITION BY mrn ORDER BY appt_date ASC) AS rn
    FROM rpt.appointment
    WHERE appt_status = 'Scheduled'
      AND appt_date >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 36, 0)
),

/****************************************************************************************
CTE: visit_total
Purpose: Calculates patient-level metrics including visit count, new patient flag,
         and 12/24-month churn indicators based on completed visits and scheduled appointments.
****************************************************************************************/
visit_total AS (
    SELECT
        v.mrn,
        COUNT(pat_enc_csn_id) AS total_visits,
        CASE 
            WHEN COUNT(CASE WHEN appt_group = 'New' THEN 1 END) > 0 AND
                 COUNT(CASE WHEN appt_group = 'Follow-Up' THEN 1 END) = 0
            THEN 1 ELSE 0
        END AS new_patient,
        CASE 
            WHEN COUNT(CASE WHEN appt_date >= DATEADD(MONTH, -12, GETDATE()) THEN 1 END) = 0
             AND NOT EXISTS (SELECT 1 FROM visit_scheduled vs WHERE vs.mrn = v.mrn)
            THEN 'TRUE' ELSE 'FALSE'
        END AS [12_month_churn],
        CASE 
            WHEN COUNT(CASE WHEN appt_date >= DATEADD(MONTH, -24, GETDATE()) THEN 1 END) = 0
             AND NOT EXISTS (SELECT 1 FROM visit_scheduled vs WHERE vs.mrn = v.mrn)
            THEN 'TRUE' ELSE 'FALSE'
        END AS [24_month_churn]
    FROM visits v
    GROUP BY v.mrn
),

/****************************************************************************************
CTE: visit_agg
Purpose: Aggregates visits per provider and specialty to support specialty-specific summaries.
****************************************************************************************/
visit_agg AS (
    SELECT 
        mrn,
        visit_prov_id,
        dept_specialty_name,
        COUNT(pat_enc_csn_id) AS visit_count,
        MIN(appt_date) AS min_appt_date
    FROM visits
    WHERE mrn IN (SELECT mrn FROM visit_total WHERE total_visits > 0)
    GROUP BY mrn, visit_prov_id, dept_specialty_name
),

/****************************************************************************************
CTE: dept_counts
Purpose: Derives specialty-level visit counts and flags PCP populations.
****************************************************************************************/
dept_counts AS (
    SELECT
        mrn,
        -- Specialty-specific visit counts
        COALESCE(SUM(CASE WHEN dept_specialty_name = 'PEDIATRICS SPECIALTIES' THEN visit_count END), 0) AS ps_visit_count,
        COALESCE(SUM(CASE WHEN dept_specialty_name = 'NEUROLOGY' THEN visit_count END), 0) AS n_visit_count,
        COALESCE(SUM(CASE WHEN dept_specialty_name = 'WOMENS HEALTH SERVICES' THEN visit_count END), 0) AS whs_visit_count,
        COALESCE(SUM(CASE WHEN dept_specialty_name = 'INTERNAL MEDICINE' THEN visit_count END), 0) AS im_visit_count,
        COALESCE(SUM(CASE WHEN dept_specialty_name = 'SURGERY NEUROLOGICAL' THEN visit_count END), 0) AS sn_visit_count,
        COALESCE(SUM(CASE WHEN dept_specialty_name = 'FAMILY MEDICINE' THEN visit_count END), 0) AS fm_visit_count,
        COALESCE(SUM(CASE WHEN dept_specialty_name = 'CARDIOLOGY' THEN visit_count END), 0) AS c_visit_count,
        COALESCE(SUM(CASE WHEN dept_specialty_name = 'GENERAL PSYCHIATRY' THEN visit_count END), 0) AS gp_visit_count,
        COALESCE(SUM(CASE WHEN dept_specialty_name = 'ENDO/MET/DIABETES' THEN visit_count END), 0) AS e_visit_count,
        COALESCE(SUM(CASE WHEN dept_specialty_name = 'PAIN MANAGEMENT' THEN visit_count END), 0) AS pm_visit_count,
        COALESCE(SUM(CASE WHEN dept_specialty_name = 'PSYCH/NEUROPSYCHOLOGY' THEN visit_count END), 0) AS p_visit_count,
        COALESCE(SUM(CASE WHEN dept_specialty_name = 'PRIMARY CARE' THEN visit_count END), 0) AS pc_visit_count,
        COALESCE(SUM(CASE WHEN dept_specialty_name = 'INFUSION' THEN visit_count END), 0) AS i_visit_count,
        COALESCE(SUM(CASE WHEN dept_specialty_name = 'RHEUMATOLOGY' THEN visit_count END), 0) AS r_visit_count,
        -- PCP cohort flags
        CASE 
            WHEN COALESCE(SUM(CASE WHEN visit_prov_id IN ('275618','269915','70133','99509','275131',
                '317218','263841','121053','125165','70397','288771','265885','103980','124380','126072','107593') 
                THEN visit_count END), 0) > 0 
            THEN 'TRUE' ELSE 'FALSE'
        END AS ucop_pcp_population,
        CASE 
            WHEN COALESCE(SUM(CASE WHEN dept_specialty_name IN 
                ('FAMILY MEDICINE', 'PEDIATRICS SPECIALTIES', 'INTERNAL MEDICINE', 'PRIMARY CARE') 
                THEN visit_count END), 0) > 0
            THEN 'TRUE' ELSE 'FALSE'
        END AS pcp_population,
        MIN(min_appt_date) AS min_appt_date
    FROM visit_agg
    GROUP BY mrn
),

/****************************************************************************************
CTE: demographics
Purpose: Gathers the most recent visit and patient demographic information, prioritizing
         visit details and supplementing with patient source tables for completeness.
****************************************************************************************/
demographics AS (
    SELECT 
        v.mrn,
        fin_class_name,
        -- Financial class categorization
        CASE 
            WHEN fin_class_name IN ('Commercial','Managed Care') THEN 'Commercial'
            WHEN fin_class_name IN ('Medicaid - California','Medicaid Managed Care') THEN 'Medicaid'
            WHEN fin_class_name IN ('Medicare','Medicare Managed Care') THEN 'Medicare'
            WHEN fin_class_name IN ('Other Government','Workers Compensation') THEN 'Other'
            WHEN fin_class_name = 'Self-Pay' THEN 'Self-Pay'
            ELSE 'Unknown'
        END AS most_recent_financial_class,
        appt_date AS most_recent_visit,
        center_name AS most_recent_visit_center,
        visit_prov_id AS most_revent_visit_prov_id,
        visit_provider_name AS most_recent_visit_provider_name,
        visit_provider_type AS most_recent_visit_provider_type,
        CASE 
            WHEN visit_provider_type LIKE '%Fellow%' OR visit_provider_type LIKE '%Resident%' 
            THEN 'TRUE' ELSE 'FALSE'
        END AS is_resident,
        -- Age buckets
        CASE 
            WHEN CAST(age AS FLOAT) < 2.0 THEN '0-2'
            WHEN CAST(age AS FLOAT) < 18.0 THEN '2-17'
            WHEN CAST(age AS FLOAT) < 36.0 THEN '18-35'
            WHEN CAST(age AS FLOAT) < 51.0 THEN '36-50'
            WHEN CAST(age AS FLOAT) < 65.0 THEN '51-64'
            WHEN CAST(age AS FLOAT) >= 65.0 THEN '65+'
            ELSE 'unknown'
        END AS age_bucket,
        CASE 
            WHEN CAST(age AS FLOAT) < 2.0 THEN '0'
            WHEN CAST(age AS FLOAT) < 18.0 THEN '1'
            WHEN CAST(age AS FLOAT) < 36.0 THEN '2'
            WHEN CAST(age AS FLOAT) < 51.0 THEN '3'
            WHEN CAST(age AS FLOAT) < 65.0 THEN '4'
            WHEN CAST(age AS FLOAT) >= 65.0 THEN '5'
            ELSE '6'
        END AS age_bucket_order,
        -- Race/gender/language
        CASE 
            WHEN race_2 IS NULL THEN race_1 
            ELSE 'Other Race or Mixed Race' 
        END AS race,
        pt.SEX_NAME AS gender,
        pl.pt_language
    FROM visits v
    LEFT JOIN ucr_health.src.patient pt ON v.mrn = pt.mrn
    LEFT JOIN ucr_health.src.patient_language pl ON v.mrn = pl.pat_mrn_id
    WHERE v.rn = 1
)

-- Final select joins all components together into a comprehensive patient profile
SELECT
    t.mrn,
    p.pat_id,
    p.patient_name,
    demo.age_bucket,
    demo.age_bucket_order,
    CASE WHEN t.new_patient = 0 THEN 'FALSE' ELSE 'TRUE' END AS new_patient,
    demo.most_recent_financial_class,
    demo.most_recent_visit,
    demo.most_recent_visit_center,
    demo.most_revent_visit_prov_id,
    demo.most_recent_visit_provider_name,
    demo.most_recent_visit_provider_type,
    demo.is_resident,
    vs.next_scheduled_appt,
    d.min_appt_date,
    t.[12_month_churn],
    t.[24_month_churn],
    -- Gender logic prioritizing cube table, then demographics
    CASE 
        WHEN p.Patient_Sex IS NULL OR p.Patient_Sex = 'Unknown' 
            THEN COALESCE(demo.gender, 'Unknown')
        ELSE p.Patient_Sex
    END AS gender,
    -- Ethnicity/Race logic
    CASE
        WHEN p.ethnic_group IN ('Mexican, Mexican American, Chicano(a)', 'Other Hispanic, Latino(a) or Spanish origin') THEN 'Hispanic or Latino'
        ELSE 
            CASE 
                WHEN p.Race IS NULL OR p.Race IN ('Unspecified Race','Unknown (Patient cannot or refuses to declare race)') 
                    THEN COALESCE(demo.race, 'Unknown (Patient cannot or refuses to declare race)')
                ELSE p.Race
            END
    END AS race,
    -- Language fallback logic
    CASE 
        WHEN p.Language IS NULL OR p.Language IN ('Unknown.', 'Unspecified Language') 
            THEN COALESCE(pt_language, 'Unknown')
        ELSE p.Language
    END AS pt_language,
    -- Counts and location data
    t.total_visits,
    d.ps_visit_count,
    d.n_visit_count,
    d.whs_visit_count,
    d.im_visit_count,
    d.sn_visit_count,
    d.fm_visit_count,
    d.c_visit_count,
    d.gp_visit_count,
    d.e_visit_count,
    d.pm_visit_count,
    d.p_visit_count,
    d.pc_visit_count,
    d.i_visit_count,
    d.r_visit_count,
    p.patient_zip_code,
    p.patient_city AS city,
    p.patient_state AS state,
    z.county,
    z.latitude,
    z.longitude,
    z.irs_estimated_population,
    d.ucop_pcp_population,
    d.pcp_population
FROM visit_total t
LEFT JOIN ucr_health.cube.dim_patient p ON t.mrn = p.patient_mrn
LEFT JOIN dept_counts d ON d.mrn = t.mrn
LEFT JOIN src.population_zipcode_irs z ON p.patient_zip_code = z.zip
LEFT JOIN demographics demo ON demo.mrn = t.mrn
LEFT JOIN visit_scheduled vs ON vs.mrn = t.mrn AND vs.rn = 1
WHERE t.mrn IS NOT NULL;
