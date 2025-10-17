-- DATA CLEANING PROJECT FOR messy_employee_table BY ANTHONY MICHAEL --

 -- OUTLINE
 -- Step 1: Create Database Table
 -- Step 2: Column to Column analysis 
 -- Step 3: Create comprehensive data quality report
 -- Step 4: Create Cleaned Employee Table
 -- Extra: Create a view for derived data
 
 -- PROJECT DESCRIPTION   
-- This project is designed and implements a comprehensive SQL data cleaning pipeline for Employee data
-- utilizing complex SQL queries for validation, standardization, and transformation 
-- created derived metrics and quality flags to ensure data integrity
-- significantly improving dataset reliability for analytical reporting and business intelligence purposes.
 
-- Step 1: Create Database Table

CREATE TABLE messy_employee_table_original AS 
SELECT * FROM messy_employee_table;

-- Verify backup
SELECT 
    'Original Table' AS table_name,
    COUNT(*) AS row_count 
FROM messy_employee_table
UNION ALL
SELECT 
    'Backup Table',
    COUNT(*) 
FROM messy_employee_table_original;

-- Step 2: Column to Column analysis 

-- Check for duplicate Employee_IDs
SELECT 
    Employee_ID, 
    COUNT(*) AS duplicate_count
FROM messy_employee_table
GROUP BY Employee_ID
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Check for NULL Employee_IDs
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN Employee_ID IS NULL THEN 1 ELSE 0 END) AS null_employee_ids,
    SUM(CASE WHEN TRIM(CAST(Employee_ID AS CHAR)) = '' THEN 1 ELSE 0 END) AS empty_employee_ids
FROM messy_employee_table;

-- Find Employee_IDs with unusual formats
SELECT 
    Employee_ID,
    LENGTH(CAST(Employee_ID AS CHAR)) AS id_length
FROM messy_employee_table
WHERE Employee_ID IS NOT NULL
  AND (
    LENGTH(CAST(Employee_ID AS CHAR)) < 3 
    OR LENGTH(CAST(Employee_ID AS CHAR)) > 15
    OR Employee_ID REGEXP '[^A-Za-z0-9]' 
  )
LIMIT 50;

-- Check for NULL and empty names
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN First_Name IS NULL THEN 1 ELSE 0 END) AS null_first_name,
    SUM(CASE WHEN Last_Name IS NULL THEN 1 ELSE 0 END) AS null_last_name,
    SUM(CASE WHEN TRIM(First_Name) = '' THEN 1 ELSE 0 END) AS empty_first_name,
    SUM(CASE WHEN TRIM(Last_Name) = '' THEN 1 ELSE 0 END) AS empty_last_name
FROM messy_employee_table;

-- Find names with leading/trailing whitespace
SELECT 
    Employee_ID,
    CONCAT('|', First_Name, '|') AS first_name_with_pipes,
    CONCAT('|', Last_Name, '|') AS last_name_with_pipes
FROM messy_employee_table
WHERE First_Name != TRIM(First_Name)
   OR Last_Name != TRIM(Last_Name)
LIMIT 20;

-- Find names with numbers or special characters
SELECT 
    Employee_ID,
    First_Name,
    Last_Name,
    'Contains Invalid Characters' AS issue
FROM messy_employee_table
WHERE First_Name REGEXP '[0-9!@#$%^&*()+={}\\[\\]:;<>?,./\\|~`]'
   OR Last_Name REGEXP '[0-9!@#$%^&*()+={}\\[\\]:;<>?,./\\|~`]'
LIMIT 30;

-- Find names with inconsistent capitalization
SELECT 
    Employee_ID,
    First_Name,
    Last_Name,
    CASE 
        WHEN First_Name = UPPER(First_Name) THEN 'First Name ALL CAPS'
        WHEN First_Name = LOWER(First_Name) THEN 'First Name all lowercase'
        ELSE 'Mixed Case'
    END AS first_name_case,
    CASE 
        WHEN Last_Name = UPPER(Last_Name) THEN 'Last Name ALL CAPS'
        WHEN Last_Name = LOWER(Last_Name) THEN 'Last Name all lowercase'
        ELSE 'Mixed Case'
    END AS last_name_case
FROM messy_employee_table
WHERE First_Name = UPPER(First_Name) 
   OR First_Name = LOWER(First_Name)
   OR Last_Name = UPPER(Last_Name)
   OR Last_Name = LOWER(Last_Name)
LIMIT 50;

-- Find unusually short or long names
SELECT 
    Employee_ID,
    First_Name,
    LENGTH(First_Name) AS first_name_length,
    Last_Name,
    LENGTH(Last_Name) AS last_name_length
FROM messy_employee_table
WHERE LENGTH(First_Name) < 2 
   OR LENGTH(First_Name) > 50
   OR LENGTH(Last_Name) < 2
   OR LENGTH(Last_Name) > 50
LIMIT 50;

-- Find placeholder or test names
SELECT 
    Employee_ID,
    First_Name,
    Last_Name
FROM messy_employee_table
WHERE LOWER(First_Name) IN ('test', 'temp', 'dummy', 'unknown', 'na', 'n/a', 'none', 'null')
   OR LOWER(Last_Name) IN ('test', 'temp', 'dummy', 'unknown', 'na', 'n/a', 'none', 'null')
LIMIT 50;

-- Basic age statistics
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN Age IS NULL THEN 1 ELSE 0 END) AS null_ages,
    MIN(Age) AS min_age,
    MAX(Age) AS max_age,
    ROUND(AVG(Age), 2) AS avg_age,
    ROUND(STDDEV(Age), 2) AS std_age
FROM messy_employee_table;

-- Find invalid ages
SELECT 
    SUM(CASE WHEN Age < 18 THEN 1 ELSE 0 END) AS under_18,
    SUM(CASE WHEN Age > 100 THEN 1 ELSE 0 END) AS over_100,
    SUM(CASE WHEN Age < 0 THEN 1 ELSE 0 END) AS negative_age,
    SUM(CASE WHEN Age = 0 THEN 1 ELSE 0 END) AS zero_age
FROM messy_employee_table;

-- View employees with unusual ages
SELECT 
    Employee_ID,
    First_Name,
    Last_Name,
    Age
FROM messy_employee_table
WHERE Age < 18 OR Age > 70 OR Age IS NULL OR Age = 0
ORDER BY Age
LIMIT 50;

-- Age distribution by ranges
SELECT 
    CASE 
        WHEN Age IS NULL THEN 'Unknown'
        WHEN Age < 18 THEN 'Under 18'
        WHEN Age BETWEEN 18 AND 24 THEN '18-24'
        WHEN Age BETWEEN 25 AND 34 THEN '25-34'
        WHEN Age BETWEEN 35 AND 44 THEN '35-44'
        WHEN Age BETWEEN 45 AND 54 THEN '45-54'
        WHEN Age BETWEEN 55 AND 64 THEN '55-64'
        WHEN Age >= 65 THEN '65+'
        ELSE 'Invalid'
    END AS age_range,
    COUNT(*) AS employee_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM messy_employee_table), 2) AS percentage
FROM messy_employee_table
GROUP BY age_range
ORDER BY 
    CASE age_range
        WHEN 'Unknown' THEN 99
        WHEN 'Under 18' THEN 0
        WHEN '18-24' THEN 1
        WHEN '25-34' THEN 2
        WHEN '35-44' THEN 3
        WHEN '45-54' THEN 4
        WHEN '55-64' THEN 5
        WHEN '65+' THEN 6
        ELSE 98
    END;

-- Check for NULL and empty values Department_Region
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN Department_Region IS NULL THEN 1 ELSE 0 END) AS null_dept_region,
    SUM(CASE WHEN TRIM(Department_Region) = '' THEN 1 ELSE 0 END) AS empty_dept_region
FROM messy_employee_table;

-- Get all unique department_region combinations
SELECT 
    Department_Region,
    COUNT(*) AS employee_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM messy_employee_table), 2) AS percentage
FROM messy_employee_table
WHERE Department_Region IS NOT NULL
GROUP BY Department_Region
ORDER BY employee_count DESC;

-- Check for whitespace issues
SELECT DISTINCT
    Department_Region,
    CONCAT('|', Department_Region, '|') AS with_pipes,
    LENGTH(Department_Region) AS original_length,
    LENGTH(TRIM(Department_Region)) AS trimmed_length
FROM messy_employee_table
WHERE Department_Region != TRIM(Department_Region)
   OR Department_Region LIKE ' %'
   OR Department_Region LIKE '% ';
   
-- Check for case inconsistencies (same value with different cases)
SELECT 
    UPPER(TRIM(Department_Region)) AS standardized,
    GROUP_CONCAT(DISTINCT Department_Region ORDER BY Department_Region SEPARATOR ' | ') AS variations,
    COUNT(DISTINCT Department_Region) AS variation_count,
    SUM(cnt) AS total_employees
FROM (
    SELECT Department_Region, COUNT(*) AS cnt
    FROM messy_employee_table
    WHERE Department_Region IS NOT NULL
    GROUP BY Department_Region
) AS dept_counts
GROUP BY UPPER(TRIM(Department_Region))
HAVING COUNT(DISTINCT Department_Region) > 1
ORDER BY total_employees DESC;

-- Analyze if Department and Region are combined
SELECT 
    Department_Region,
    CASE 
        WHEN Department_Region LIKE '%-%' THEN SUBSTRING_INDEX(Department_Region, '-', 1)
        ELSE Department_Region
    END AS possible_department,
    CASE 
        WHEN Department_Region LIKE '%-%' THEN SUBSTRING_INDEX(Department_Region, '-', -1)
        ELSE NULL
    END AS possible_region,
    COUNT(*) AS count
FROM messy_employee_table
WHERE Department_Region IS NOT NULL
GROUP BY Department_Region
ORDER BY count DESC
LIMIT 30;

-- Find potential typos or misspellings
SELECT 
    Department_Region,
    COUNT(*) AS count
FROM messy_employee_table
WHERE Department_Region IS NOT NULL
GROUP BY Department_Region
HAVING COUNT(*) < 5  -- Departments with very few employees might be typos
ORDER BY Department_Region;

-- Get all unique statuses
SELECT 
    Status,
    COUNT(*) AS employee_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM messy_employee_table), 2) AS percentage
FROM messy_employee_table
WHERE Status IS NOT NULL
GROUP BY Status
ORDER BY employee_count DESC;

-- Check for NULL and empty statuses
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN Status IS NULL THEN 1 ELSE 0 END) AS null_status,
    SUM(CASE WHEN TRIM(Status) = '' THEN 1 ELSE 0 END) AS empty_status
FROM messy_employee_table;

-- Check for case variations of the same status
SELECT 
    UPPER(TRIM(Status)) AS standardized_status,
    GROUP_CONCAT(DISTINCT Status ORDER BY Status SEPARATOR ' | ') AS variations,
    COUNT(DISTINCT Status) AS variation_count,
    SUM(cnt) AS total_employees
FROM (
    SELECT Status, COUNT(*) AS cnt
    FROM messy_employee_table
    WHERE Status IS NOT NULL
    GROUP BY Status
) AS status_counts
GROUP BY UPPER(TRIM(Status))
HAVING COUNT(DISTINCT Status) > 1
ORDER BY total_employees DESC;

-- Find unexpected or invalid status values
SELECT DISTINCT 
    Status,
    COUNT(*) AS count
FROM messy_employee_table
WHERE UPPER(TRIM(Status)) NOT IN (
    'ACTIVE', 'INACTIVE', 'PENDING'
)
AND Status IS NOT NULL
GROUP BY Status
ORDER BY count DESC;

-- Check status distribution with whitespace
SELECT 
    Status,
    CONCAT('|', Status, '|') AS with_pipes,
    COUNT(*) AS count
FROM messy_employee_table
WHERE Status != TRIM(Status)
GROUP BY Status;

-- Basic date statistics
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN Join_Date IS NULL THEN 1 ELSE 0 END) AS null_dates,
    MIN(Join_Date) AS earliest_join_date,
    MAX(Join_Date) AS latest_join_date,
    COUNT(DISTINCT Join_Date) AS unique_join_dates
FROM messy_employee_table;

-- Find future join dates
SELECT 
    Employee_ID,
    First_Name,
    Last_Name,
    Join_Date,
    DATEDIFF(Join_Date, CURDATE()) AS join_in_future
FROM messy_employee_table
WHERE Join_Date > CURDATE()
ORDER BY Join_Date DESC
;

-- Find unrealistic old dates
SELECT 
    Employee_ID,
    First_Name,
    Last_Name,
    Join_Date,
    TIMESTAMPDIFF(YEAR, Join_Date, CURDATE()) AS years_ago
FROM messy_employee_table
WHERE Join_Date < '1950-01-01'
ORDER BY Join_Date
;

-- Find placeholder dates
SELECT 
    Join_Date,
    COUNT(*) AS count
FROM messy_employee_table
WHERE Join_Date IN (
    '1900-01-01', '1970-01-01', '2000-01-01', 
    '1999-12-31', '1111-11-11', '0000-00-00'
)
GROUP BY Join_Date;

-- Check age vs join date consistency
SELECT 
    Employee_ID,
    First_Name,
    Last_Name,
    Age,
    Join_Date,
    TIMESTAMPDIFF(YEAR, Join_Date, CURDATE()) AS years_of_service,
    Age - TIMESTAMPDIFF(YEAR, Join_Date, CURDATE()) AS age_at_joining,
    CASE 
        WHEN TIMESTAMPDIFF(YEAR, Join_Date, CURDATE()) > Age THEN 'Joined before birth!'
        WHEN Age - TIMESTAMPDIFF(YEAR, Join_Date, CURDATE()) < 16 THEN 'Joined too young (under 16)'
        WHEN Age - TIMESTAMPDIFF(YEAR, Join_Date, CURDATE()) < 18 THEN 'Joined as minor (under 18)'
        ELSE 'OK'
    END AS consistency_check
FROM messy_employee_table
WHERE Join_Date IS NOT NULL 
  AND Age IS NOT NULL
HAVING consistency_check != 'OK'
ORDER BY age_at_joining
;

-- Find employees with same join date (suspicious pattern)
SELECT 
    Join_Date,
    COUNT(*) AS employee_count,
    GROUP_CONCAT(Employee_ID ORDER BY Employee_ID SEPARATOR ', ') AS employee_ids
FROM messy_employee_table
WHERE Join_Date IS NOT NULL
GROUP BY Join_Date
HAVING COUNT(*) > 2  -- More than 2 employees joined on same day
ORDER BY employee_count DESC;

-- Basic salary statistics
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN Salary IS NULL THEN 1 ELSE 0 END) AS null_salaries,
    MIN(Salary) AS min_salary,
    MAX(Salary) AS max_salary,
    ROUND(AVG(Salary), 2) AS avg_salary,
    ROUND(STDDEV(Salary), 2) AS std_salary
FROM messy_employee_table
WHERE Salary IS NOT NULL;

-- Find invalid salaries
SELECT 
    SUM(CASE WHEN Salary <= 0 THEN 1 ELSE 0 END) AS zero_or_negative_salary,
    SUM(CASE WHEN Salary < 10000 THEN 1 ELSE 0 END) AS unrealistically_low,
    SUM(CASE WHEN Salary > 1000000 THEN 1 ELSE 0 END) AS extremely_high,
    SUM(CASE WHEN Salary BETWEEN 1 AND 100 THEN 1 ELSE 0 END) AS possibly_in_thousands
FROM messy_employee_table
WHERE Salary IS NOT NULL;

-- View employees with unusual salaries
SELECT 
    Employee_ID,
    First_Name,
    Last_Name,
    Department_Region,
    Salary,
    CASE 
        WHEN Salary <= 0 THEN 'Zero or Negative'
        WHEN Salary < 10000 THEN 'Too Low'
        WHEN Salary > 500000 THEN 'Extremely High'
        WHEN Salary BETWEEN 1 AND 100 THEN 'Possibly in Thousands'
        ELSE 'Check Manually'
    END AS issue_type
FROM messy_employee_table
WHERE Salary <= 0 
   OR Salary < 10000 
   OR Salary > 500000
   OR Salary BETWEEN 1 AND 100
ORDER BY Salary
;

-- Salary distribution by ranges
SELECT 
    CASE 
        WHEN Salary IS NULL THEN 'Unknown'
        WHEN Salary <= 0 THEN 'Invalid (<=0)'
        WHEN Salary < 30000 THEN 'Under $30K'
        WHEN Salary BETWEEN 30000 AND 49999 THEN '$30K-$50K'
        WHEN Salary BETWEEN 50000 AND 74999 THEN '$50K-$75K'
        WHEN Salary BETWEEN 75000 AND 99999 THEN '$75K-$100K'
        WHEN Salary BETWEEN 100000 AND 149999 THEN '$100K-$150K'
        WHEN Salary BETWEEN 150000 AND 199999 THEN '$150K-$200K'
        WHEN Salary >= 200000 THEN '$200K+'
        ELSE 'Other'
    END AS salary_range,
    COUNT(*) AS employee_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM messy_employee_table), 2) AS percentage
FROM messy_employee_table
GROUP BY salary_range
ORDER BY 
    CASE salary_range
        WHEN 'Invalid (<=0)' THEN 0
        WHEN 'Under $30K' THEN 1
        WHEN '$30K-$50K' THEN 2
        WHEN '$50K-$75K' THEN 3
        WHEN '$75K-$100K' THEN 4
        WHEN '$100K-$150K' THEN 5
        WHEN '$150K-$200K' THEN 6
        WHEN '$200K+' THEN 7
        WHEN 'Unknown' THEN 98
        ELSE 99
    END;

-- Salary statistics by department_region
SELECT 
    Department_Region,
    COUNT(*) AS employee_count,
    MIN(Salary) AS min_salary,
    MAX(Salary) AS max_salary,
    ROUND(AVG(Salary), 2) AS avg_salary,
    ROUND(STDDEV(Salary), 2) AS std_salary,
    ROUND(AVG(Salary) - STDDEV(Salary), 2) AS lower_range,
    ROUND(AVG(Salary) + STDDEV(Salary), 2) AS upper_range
FROM messy_employee_table
WHERE Salary IS NOT NULL 
  AND Salary > 0
  AND Department_Region IS NOT NULL
GROUP BY Department_Region
ORDER BY avg_salary DESC;

-- Find duplicate salaries (suspicious pattern)
SELECT 
    Salary,
    COUNT(*) AS employee_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM messy_employee_table WHERE Salary IS NOT NULL), 2) AS percentage
FROM messy_employee_table
WHERE Salary IS NOT NULL
GROUP BY Salary
HAVING COUNT(*) > 5
ORDER BY employee_count DESC
;

-- Check for NULL and empty emails
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN Email IS NULL THEN 1 ELSE 0 END) AS null_emails,
    SUM(CASE WHEN TRIM(Email) = '' THEN 1 ELSE 0 END) AS empty_emails
FROM messy_employee_table;

-- Validate email format (basic regex)
SELECT 
    Employee_ID,
    First_Name,
    Last_Name,
    Email,
    CASE 
        WHEN Email NOT LIKE '%@%' THEN 'Missing @'
        WHEN Email NOT LIKE '%@%.%' THEN 'Missing domain'
        WHEN Email LIKE '%@%@%' THEN 'Multiple @ symbols'
        WHEN Email LIKE ' %' OR Email LIKE '% ' THEN 'Has leading/trailing whitespace'
        WHEN Email REGEXP '[A-Z]' THEN 'Contains uppercase letters'
        WHEN Email LIKE '%..%' THEN 'Has consecutive dots'
        WHEN Email LIKE '.%@%' OR Email LIKE '%@.%' THEN 'Dot adjacent to @'
        WHEN Email LIKE '@%' OR Email LIKE '%@' THEN 'Starts or ends with @'
        WHEN Email REGEXP '[^a-z0-9.@_-]' THEN 'Contains invalid characters'
        ELSE 'Other format issue'
    END AS email_issue
FROM messy_employee_table
WHERE Email IS NOT NULL
  AND TRIM(Email) != ''
  AND Email NOT REGEXP '^[a-z0-9._%-]+@[a-z0-9.-]+\.[a-z]{2,}$'
;

-- Find duplicate emails
SELECT 
    LOWER(TRIM(Email)) AS standardized_email,
    COUNT(*) AS duplicate_count,
    GROUP_CONCAT(Employee_ID ORDER BY Employee_ID SEPARATOR ', ') AS employee_ids
FROM messy_employee_table
WHERE Email IS NOT NULL
GROUP BY LOWER(TRIM(Email))
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Analyze email domains
SELECT 
    LOWER(SUBSTRING_INDEX(Email, '@', -1)) AS email_domain,
    COUNT(*) AS employee_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM messy_employee_table WHERE Email LIKE '%@%'), 2) AS percentage
FROM messy_employee_table
WHERE Email IS NOT NULL 
  AND Email LIKE '%@%'
GROUP BY email_domain
ORDER BY employee_count DESC;

-- Find personal email domains (non-corporate)
SELECT 
    Employee_ID,
    First_Name,
    Last_Name,
    Email,
    LOWER(SUBSTRING_INDEX(Email, '@', -1)) AS email_domain
FROM messy_employee_table
WHERE LOWER(SUBSTRING_INDEX(Email, '@', -1)) IN (
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 
    'aol.com', 'icloud.com', 'mail.com', 'protonmail.com'
)
;

-- Find generic or placeholder emails
SELECT 
    Employee_ID,
    First_Name,
    Last_Name,
    Email,
    'Placeholder/Generic Email' AS issue
FROM messy_employee_table
WHERE LOWER(Email) LIKE '%test%'
   OR LOWER(Email) LIKE '%temp%'
   OR LOWER(Email) LIKE '%dummy%'
   OR LOWER(Email) LIKE '%fake%'
   OR LOWER(Email) LIKE '%sample%'
   OR LOWER(Email) IN ('n/a', 'na', 'none', 'null', 'tbd', 'unknown')
   OR Email = 'email@example.com'
LIMIT 30;

-- Find emails with whitespace
SELECT 
    Employee_ID,
    Email,
    CONCAT('|', Email, '|') AS with_pipes
FROM messy_employee_table
WHERE Email != TRIM(Email)
   OR Email LIKE '% %'
LIMIT 30;

-- Check for NULL and empty phone numbers
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN Phone IS NULL THEN 1 ELSE 0 END) AS null_phones,
    SUM(CASE WHEN TRIM(Phone) = '' THEN 1 ELSE 0 END) AS empty_phones
FROM messy_employee_table;

-- Analyze phone number formats and lengths
SELECT 
    Phone,
    LENGTH(Phone) AS phone_length,
    LENGTH(REGEXP_REPLACE(Phone, '[^0-9]', '')) AS digit_count,
    COUNT(*) AS count
FROM messy_employee_table
WHERE Phone IS NOT NULL 
  AND TRIM(Phone) != ''
GROUP BY Phone, phone_length
ORDER BY count DESC
LIMIT 30;

-- Find phone numbers with unusual digit counts
SELECT 
    Employee_ID,
    First_Name,
    Last_Name,
    Phone,
    LENGTH(REGEXP_REPLACE(Phone, '[^0-9]', '')) AS digit_count,
    CASE
WHEN LENGTH(REGEXP_REPLACE(Phone, '[^0-9]', '')) < 9 THEN 'Too Few Digits'
        WHEN LENGTH(REGEXP_REPLACE(Phone, '[^0-9]', '')) > 11 THEN 'Too Many Digits'
        ELSE 'Unusual Length'
    END AS issue
FROM messy_employee_table
WHERE Phone IS NOT NULL
  AND (
    LENGTH(REGEXP_REPLACE(Phone, '[^0-9]', '')) < 9 
    OR LENGTH(REGEXP_REPLACE(Phone, '[^0-9]', '')) > 11
  )
ORDER BY digit_count
;

-- Find duplicate phone numbers
SELECT 
    REGEXP_REPLACE(Phone, '[^0-9]', '') AS phone_digits_only,
    COUNT(*) AS duplicate_count,
    GROUP_CONCAT(DISTINCT Phone ORDER BY Phone SEPARATOR ' | ') AS phone_variations,
    GROUP_CONCAT(Employee_ID ORDER BY Employee_ID SEPARATOR ', ') AS employee_ids
FROM messy_employee_table
WHERE Phone IS NOT NULL
  AND TRIM(Phone) != ''
GROUP BY REGEXP_REPLACE(Phone, '[^0-9]', '')
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
;

-- Find placeholder phone numbers
SELECT 
    Employee_ID,
    First_Name,
    Last_Name,
    Phone,
    'Placeholder Phone' AS issue
FROM messy_employee_table
WHERE Phone IN (
    '0000000000', '1111111111', '9999999999', 
    '000-000-0000', '111-111-1111', '999-999-9999',
    '0', '1', 'N/A', 'NA', 'None', 'null', 'TBD', 'Unknown'
  )
   OR Phone REGEXP '^0+$'
   OR Phone REGEXP '^1+$'
   OR Phone REGEXP '^9+$'
   OR Phone LIKE '555-555-%'
   OR Phone LIKE '%000-0000'
;

-- Analyze phone format patterns
SELECT 
    CASE 
        WHEN Phone REGEXP '^[0-9]{3}-[0-9]{3}-[0-9]{4}$' THEN '###-###-####'
        WHEN Phone REGEXP '^\([0-9]{3}\) [0-9]{3}-[0-9]{4}$' THEN '(###) ###-####'
        WHEN Phone REGEXP '^\+1[0-9]{10}$' THEN '+1##########'
        WHEN Phone REGEXP '^\+[0-9]{1,3} [0-9]{10}$' THEN '+# ##########'
        WHEN Phone REGEXP '^[0-9]{10}$' THEN '##########'
        WHEN Phone REGEXP '^[0-9]{3}\.[0-9]{3}\.[0-9]{4}$' THEN '###.###.####'
        WHEN Phone REGEXP '^1-[0-9]{3}-[0-9]{3}-[0-9]{4}$' THEN '1-###-###-####'
        ELSE 'Other/Invalid Format'
    END AS phone_format,
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM messy_employee_table WHERE Phone IS NOT NULL), 2) AS percentage
FROM messy_employee_table
WHERE Phone IS NOT NULL 
  AND TRIM(Phone) != ''
GROUP BY phone_format
ORDER BY count DESC;

-- Find phones with letters or special characters
SELECT 
    Employee_ID,
    First_Name,
    Last_Name,
    Phone,
    'Contains Letters/Invalid Chars' AS issue
FROM messy_employee_table
WHERE Phone REGEXP '[A-Za-z!@#$%^&*()+={}\\[\\]:;<>?,./\\|~`]'
  AND Phone IS NOT NULL
;

SELECT DISTINCT performance_Score 
FROM messy_employee_table;

-- -- Get all unique performance scores
SELECT Performance_Score, COUNT(*) as Count
FROM messy_employee_table
GROUP BY Performance_Score;

-- Find unexpected text performance score values
SELECT DISTINCT 
    Performance_Score,
    COUNT(*) AS count
FROM messy_employee_table
WHERE Performance_Score IS NOT NULL
  AND UPPER(TRIM(Performance_Score)) NOT IN (
    'FAIR', 'OUTSTANDING', 'SATISFACTORY', 'UNSATISFACTORY',
    'EXCEEDS EXPECTATIONS', 'MEETS EXPECTATIONS', 'NEEDS IMPROVEMENT',
    'A', 'B', 'C', 'D', 'F', '5', '4', '3', '2', '1'
  )
GROUP BY Performance_Score
ORDER BY count DESC;

-- Check for whitespace in performance scores
SELECT 
    Performance_Score,
    CONCAT('|', Performance_Score, '|') AS with_pipes,
    COUNT(*) AS count
FROM messy_employee_table
WHERE Performance_Score != TRIM(Performance_Score)
GROUP BY Performance_Score
;

-- Check for NULL values in Remote Work
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN Remote_Work IS NULL THEN 1 ELSE 0 END) AS null_remote_work,
    SUM(CASE WHEN TRIM(CAST(Remote_Work AS CHAR)) = '' THEN 1 ELSE 0 END) AS empty_remote_work
FROM messy_employee_table;

-- Get all unique remote work values
SELECT 
    Remote_Work,
    COUNT(*) AS employee_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM messy_employee_table WHERE Remote_Work IS NOT NULL), 2) AS percentage
FROM messy_employee_table
WHERE Remote_Work IS NOT NULL
GROUP BY Remote_Work
ORDER BY employee_count DESC;

-- Check data type and standardize
SELECT DISTINCT 
    Remote_Work,
    CASE 
        WHEN UPPER(TRIM(Remote_Work)) IN ('TRUE') THEN 'Yes'
        WHEN UPPER(TRIM(Remote_Work)) IN ('FALSE') THEN 'No'
        ELSE 'Unknown/Other'
    END AS standardized_value,
    COUNT(*) AS count
FROM messy_employee_table
WHERE Remote_Work IS NOT NULL
GROUP BY Remote_Work, standardized_value
ORDER BY count DESC;

-- Find unexpected remote work values
SELECT DISTINCT 
    Remote_Work,
    COUNT(*) AS count
FROM messy_employee_table
WHERE Remote_Work IS NOT NULL
  AND UPPER(TRIM(Remote_Work)) NOT IN (
    'YES', 'NO', 'Y', 'N', '1', '0',
    'REMOTE', 'OFFICE', 'HYBRID', 'PARTIAL', 'FULL', 'NONE'
  )
GROUP BY Remote_Work
ORDER BY count DESC;

-- Remote work by department
SELECT 
    Department_Region,
    Remote_Work,
    COUNT(*) AS employee_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY Department_Region), 2) AS dept_percentage
FROM messy_employee_table
WHERE Department_Region IS NOT NULL
  AND Remote_Work IS NOT NULL
GROUP BY Department_Region, Remote_Work
ORDER BY Department_Region, employee_count DESC;

-- Remote work by status
SELECT 
    Status,
    Remote_Work,
    COUNT(*) AS employee_count
FROM messy_employee_table
WHERE Status IS NOT NULL
  AND Remote_Work IS NOT NULL
GROUP BY Status, Remote_Work
ORDER BY Status, employee_count DESC;



-- Step 3: Create comprehensive data quality report

CREATE TEMPORARY TABLE data_quality_report AS
SELECT 
    'Total Rows' AS metric_category,
    'Count' AS metric_name,
    COUNT(*) AS metric_value,
    'N/A' AS percentage
FROM messy_employee_table

UNION ALL

-- Employee_ID metrics
SELECT 'Employee_ID', 'NULL Count', 
    SUM(CASE WHEN Employee_ID IS NULL THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN Employee_ID IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table

UNION ALL

SELECT 'Employee_ID', 'Duplicates', 
    COUNT(*) - COUNT(DISTINCT Employee_ID),
    CONCAT(ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT Employee_ID)) / COUNT(*), 2), '%')
FROM messy_employee_table

UNION ALL

-- First_Name metrics
SELECT 'First_Name', 'NULL/Empty', 
    SUM(CASE WHEN First_Name IS NULL OR TRIM(First_Name) = '' THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN First_Name IS NULL OR TRIM(First_Name) = '' THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table

UNION ALL

SELECT 'First_Name', 'With Numbers/Special Chars',
    SUM(CASE WHEN First_Name REGEXP '[0-9!@#$%^&*()+={}\\[\\]:;<>?,./\\\\|~`]' THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN First_Name REGEXP '[0-9!@#$%^&*()+={}\\[\\]:;<>?,./\\\\|~`]' THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table

UNION ALL

-- Last_Name metrics
SELECT 'Last_Name', 'NULL/Empty', 
    SUM(CASE WHEN Last_Name IS NULL OR TRIM(Last_Name) = '' THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN Last_Name IS NULL OR TRIM(Last_Name) = '' THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table

UNION ALL

-- Age metrics
SELECT 'Age', 'NULL Count', 
    SUM(CASE WHEN Age IS NULL THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN Age IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table

UNION ALL

SELECT 'Age', 'Invalid (<18 or >100)', 
    SUM(CASE WHEN Age < 18 OR Age > 100 THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN Age < 18 OR Age > 100 THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table

UNION ALL

-- Department_Region metrics
SELECT 'Department_Region', 'NULL/Empty', 
    SUM(CASE WHEN Department_Region IS NULL OR TRIM(Department_Region) = '' THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN Department_Region IS NULL OR TRIM(Department_Region) = '' THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table

UNION ALL

SELECT 'Department_Region', 'Unique Values',
    COUNT(DISTINCT Department_Region),
    'N/A'
FROM messy_employee_table

UNION ALL

-- Status metrics
SELECT 'Status', 'NULL/Empty', 
    SUM(CASE WHEN Status IS NULL OR TRIM(Status) = '' THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN Status IS NULL OR TRIM(Status) = '' THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table

UNION ALL

SELECT 'Status', 'Unique Values',
    COUNT(DISTINCT Status),
    'N/A'
FROM messy_employee_table

UNION ALL

-- Join_Date metrics
SELECT 'Join_Date', 'NULL Count', 
    SUM(CASE WHEN Join_Date IS NULL THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN Join_Date IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table

UNION ALL

SELECT 'Join_Date', 'Future Dates', 
    SUM(CASE WHEN STR_TO_DATE(Join_Date, '%m/%d/%Y') > CURDATE() THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN STR_TO_DATE(Join_Date, '%m/%d/%Y') > CURDATE() THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table

UNION ALL

SELECT 'Join_Date', 'Before 1950',
    SUM(CASE WHEN STR_TO_DATE(Join_Date, '%m/%d/%Y') < '1950-01-01' THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN STR_TO_DATE(Join_Date, '%m/%d/%Y') < '1950-01-01' THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table

UNION ALL

-- Salary metrics
SELECT 'Salary', 'NULL Count', 
    SUM(CASE WHEN Salary IS NULL THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN Salary IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table

UNION ALL

SELECT 'Salary', 'Invalid (<=0)', 
    SUM(CASE WHEN Salary <= 0 THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN Salary <= 0 THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table

UNION ALL

SELECT 'Salary', 'Unrealistically Low (<$10K)',
    SUM(CASE WHEN Salary < 10000 AND Salary > 0 THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN Salary < 10000 AND Salary > 0 THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table

UNION ALL

-- Email metrics
SELECT 'Email', 'NULL/Empty', 
    SUM(CASE WHEN Email IS NULL OR TRIM(Email) = '' THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN Email IS NULL OR TRIM(Email) = '' THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table

UNION ALL

SELECT 'Email', 'Invalid Format', 
    SUM(CASE WHEN Email NOT REGEXP '^[a-z0-9._%-]+@[a-z0-9.-]+\.[a-z]{2,}$' 
             AND Email IS NOT NULL AND TRIM(Email) != '' THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN Email NOT REGEXP '^[a-z0-9._%-]+@[a-z0-9.-]+\.[a-z]{2,}$' 
                                   AND Email IS NOT NULL AND TRIM(Email) != '' THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table

UNION ALL

SELECT 'Email', 'Duplicates',
    COUNT(*) - COUNT(DISTINCT LOWER(TRIM(Email))),
    CONCAT(ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT LOWER(TRIM(Email)))) / COUNT(*), 2), '%')
FROM messy_employee_table
WHERE Email IS NOT NULL

UNION ALL

-- Phone metrics
SELECT 'Phone', 'NULL/Empty', 
    SUM(CASE WHEN Phone IS NULL OR TRIM(Phone) = '' THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN Phone IS NULL OR TRIM(Phone) = '' THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table

UNION ALL

SELECT 'Phone', 'Invalid Length',
    SUM(CASE WHEN LENGTH(REGEXP_REPLACE(Phone, '[^0-9]', '')) < 10 
             OR LENGTH(REGEXP_REPLACE(Phone, '[^0-9]', '')) > 15 THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN LENGTH(REGEXP_REPLACE(Phone, '[^0-9]', '')) < 10 
                                   OR LENGTH(REGEXP_REPLACE(Phone, '[^0-9]', '')) > 15 THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table
WHERE Phone IS NOT NULL

UNION ALL

-- Performance_Score metrics
SELECT 'Performance_Score', 'NULL Count', 
    SUM(CASE WHEN Performance_Score IS NULL THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN Performance_Score IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table

UNION ALL

-- Remote_Work metrics
SELECT 'Remote_Work', 'NULL Count', 
    SUM(CASE WHEN Remote_Work IS NULL THEN 1 ELSE 0 END),
    CONCAT(ROUND(100.0 * SUM(CASE WHEN Remote_Work IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2), '%')
FROM messy_employee_table;

-- View the report
SELECT * FROM data_quality_report;


-- Step 4: Create Cleaned Employee Table

-- Create the cleaned employee table with all transformations
CREATE TABLE employees_cleaned AS
SELECT 
    -- Employee_ID: Keep as is (primary key)
    Employee_ID,
    
    -- First_Name: Trim, proper case, remove invalid characters
    CASE 
        WHEN First_Name IS NULL OR TRIM(First_Name) = '' THEN NULL
        WHEN First_Name REGEXP '[0-9!@#$%^&*()+={}\\[\\]:;<>?,./\\\\|~`]' THEN NULL
        ELSE TRIM(CONCAT(
            UPPER(SUBSTRING(First_Name, 1, 1)), 
            LOWER(SUBSTRING(First_Name, 2))
        ))
    END AS First_Name,
    
    -- Last_Name: Trim, proper case, remove invalid characters
    CASE 
        WHEN Last_Name IS NULL OR TRIM(Last_Name) = '' THEN NULL
        WHEN Last_Name REGEXP '[0-9!@#$%^&*()+={}\\[\\]:;<>?,./\\\\|~`]' THEN NULL
        ELSE TRIM(CONCAT(
            UPPER(SUBSTRING(Last_Name, 1, 1)), 
            LOWER(SUBSTRING(Last_Name, 2))
        ))
    END AS Last_Name,
    
    -- Age: Set invalid ages to NULL
    CASE 
        WHEN Age IS NULL THEN NULL
        WHEN Age < 18 OR Age > 100 THEN NULL
        WHEN Age = 0 THEN NULL
        ELSE Age
    END AS Age,
    
    -- Department_Region: Trim and standardize
    CASE 
        WHEN Department_Region IS NULL OR TRIM(Department_Region) = '' THEN NULL
        ELSE TRIM(Department_Region)
    END AS Department_Region,
    
    -- Status: Standardize to consistent values
    CASE 
        WHEN Status IS NULL OR TRIM(Status) = '' THEN NULL
        WHEN UPPER(TRIM(Status)) IN ('ACTIVE', 'EMPLOYED', 'WORKING', 'CURRENT') THEN 'Active'
        WHEN UPPER(TRIM(Status)) IN ('INACTIVE', 'NOT ACTIVE', 'NOTACTIVE') THEN 'Inactive'
        WHEN UPPER(TRIM(Status)) IN ('ON LEAVE', 'LEAVE', 'ONLEAVE', 'ON-LEAVE') THEN 'On Leave'
        WHEN UPPER(TRIM(Status)) IN ('TERMINATED', 'FIRED', 'DISMISSED') THEN 'Terminated'
        WHEN UPPER(TRIM(Status)) IN ('RESIGNED', 'QUIT', 'LEFT') THEN 'Resigned'
        WHEN UPPER(TRIM(Status)) IN ('RETIRED', 'RETIREMENT') THEN 'Retired'
        ELSE TRIM(CONCAT(
            UPPER(SUBSTRING(Status, 1, 1)), 
            LOWER(SUBSTRING(Status, 2))
        ))
    END AS Status,
    
    -- Join_Date: Handle multiple date formats and set invalid dates to NULL
    CASE 
        WHEN Join_Date IS NULL THEN NULL
        WHEN STR_TO_DATE(Join_Date, '%m/%d/%Y') IS NOT NULL THEN STR_TO_DATE(Join_Date, '%m/%d/%Y')
        WHEN STR_TO_DATE(Join_Date, '%Y-%m-%d') IS NOT NULL THEN STR_TO_DATE(Join_Date, '%Y-%m-%d')
        WHEN STR_TO_DATE(Join_Date, '%d-%m-%Y') IS NOT NULL THEN STR_TO_DATE(Join_Date, '%d-%m-%Y')
        ELSE NULL
    END AS Join_Date,
    
    -- Salary: Set invalid salaries to NULL
    CASE 
        WHEN Salary IS NULL THEN NULL
        WHEN Salary <= 0 THEN NULL
        WHEN Salary < 10000 THEN NULL  -- Unrealistically low
        WHEN Salary > 1000000 THEN NULL  -- Unrealistically high (adjust threshold as needed)
        ELSE Salary
    END AS Salary,
    
    -- Email: Lowercase, trim, validate format
    CASE 
        WHEN Email IS NULL OR TRIM(Email) = '' THEN NULL
        WHEN Email NOT REGEXP '^[a-z0-9._%-]+@[a-z0-9.-]+\.[a-z]{2,}$' THEN NULL
        WHEN LOWER(TRIM(Email)) IN ('n/a', 'na', 'none', 'null', 'tbd', 'test@test.com', 'email@example.com') THEN NULL
        WHEN LOWER(Email) LIKE '%test%' OR LOWER(Email) LIKE '%temp%' OR LOWER(Email) LIKE '%dummy%' THEN NULL
        ELSE LOWER(TRIM(Email))
    END AS Email,
    
    -- Phone: Keep only valid formats, standardize
    CASE 
        WHEN Phone IS NULL OR TRIM(Phone) = '' THEN NULL
        WHEN Phone IN ('0000000000', '1111111111', '9999999999', 'N/A', 'NA', 'None') THEN NULL
        WHEN LENGTH(REGEXP_REPLACE(Phone, '[^0-9]', '')) < 10 THEN NULL
        WHEN LENGTH(REGEXP_REPLACE(Phone, '[^0-9]', '')) > 15 THEN NULL
        ELSE TRIM(Phone)
    END AS Phone,
    
    -- Performance_Score: Standardize based on type
    CASE 
        WHEN Performance_Score IS NULL OR TRIM(CAST(Performance_Score AS CHAR)) = '' THEN NULL
        -- If numeric, validate range (assuming 1-5 scale)
        WHEN Performance_Score REGEXP '^[0-9.]+$' 
             AND (CAST(Performance_Score AS DECIMAL(10,2)) < 1 
                  OR CAST(Performance_Score AS DECIMAL(10,2)) > 5) THEN NULL
        -- If text, standardize
        WHEN UPPER(TRIM(Performance_Score)) IN ('EXCELLENT', 'OUTSTANDING', '5', 'A') THEN 'Excellent'
        WHEN UPPER(TRIM(Performance_Score)) IN ('GOOD', 'ABOVE AVERAGE', '4', 'B') THEN 'Good'
        WHEN UPPER(TRIM(Performance_Score)) IN ('AVERAGE', 'SATISFACTORY', '3', 'C') THEN 'Average'
        WHEN UPPER(TRIM(Performance_Score)) IN ('FAIR', 'BELOW AVERAGE', '2', 'D') THEN 'Fair'
        WHEN UPPER(TRIM(Performance_Score)) IN ('POOR', 'UNSATISFACTORY', '1', 'F') THEN 'Poor'
        ELSE Performance_Score
    END AS Performance_Score,
    
    -- Remote_Work: Standardize to Yes/No/Hybrid
    CASE 
        WHEN Remote_Work IS NULL OR TRIM(CAST(Remote_Work AS CHAR)) = '' THEN NULL
        WHEN UPPER(TRIM(Remote_Work)) IN ('YES', 'Y', 'TRUE', '1', 'REMOTE', 'FULL REMOTE', 'FULLY REMOTE', 'FULL') THEN 'Yes'
        WHEN UPPER(TRIM(Remote_Work)) IN ('NO', 'N', 'FALSE', '0', 'OFFICE', 'ON-SITE', 'ONSITE', 'NONE') THEN 'No'
        WHEN UPPER(TRIM(Remote_Work)) IN ('HYBRID', 'PARTIAL', 'SOMETIMES', 'FLEXIBLE', 'MIXED', 'PART TIME', 'PART-TIME') THEN 'Hybrid'
        ELSE 'Unknown'
    END AS Remote_Work,
    
    -- Add data quality flags
    CASE 
        WHEN Age < 18 OR Age > 100 OR Age IS NULL THEN 1 
        ELSE 0 
    END AS age_flag,
    
    CASE 
        WHEN Salary <= 0 OR Salary IS NULL OR Salary < 10000 THEN 1 
        ELSE 0 
    END AS salary_flag,
    
    CASE 
        WHEN Email IS NULL OR Email NOT REGEXP '^[a-z0-9._%-]+@[a-z0-9.-]+\.[a-z]{2,}$' THEN 1 
        ELSE 0 
    END AS email_flag,
    
    CASE 
        WHEN Phone IS NULL OR LENGTH(REGEXP_REPLACE(Phone, '[^0-9]', '')) < 10 THEN 1 
        ELSE 0 
    END AS phone_flag,
    
    -- Add record quality score (lower is better)
    (CASE WHEN Employee_ID IS NULL THEN 1 ELSE 0 END +
     CASE WHEN First_Name IS NULL OR TRIM(First_Name) = '' THEN 1 ELSE 0 END +
     CASE WHEN Last_Name IS NULL OR TRIM(Last_Name) = '' THEN 1 ELSE 0 END +
     CASE WHEN Age IS NULL OR Age < 18 OR Age > 100 THEN 1 ELSE 0 END +
     CASE WHEN Department_Region IS NULL OR TRIM(Department_Region) = '' THEN 1 ELSE 0 END +
     CASE WHEN Status IS NULL OR TRIM(Status) = '' THEN 1 ELSE 0 END +
     CASE WHEN Join_Date IS NULL THEN 1 ELSE 0 END +  -- Modified this line
     CASE WHEN Salary IS NULL OR Salary <= 0 THEN 1 ELSE 0 END +
     CASE WHEN Email IS NULL OR Email NOT REGEXP '^[a-z0-9._%-]+@[a-z0-9.-]+\.[a-z]{2,}$' THEN 1 ELSE 0 END +
     CASE WHEN Phone IS NULL THEN 1 ELSE 0 END +
     CASE WHEN Performance_Score IS NULL THEN 1 ELSE 0 END +
     CASE WHEN Remote_Work IS NULL THEN 1 ELSE 0 END
    ) AS data_quality_score

FROM messy_employee_table
WHERE Employee_ID IS NOT NULL  -- Must have Employee_ID
  AND (First_Name IS NOT NULL AND TRIM(First_Name) != '')  -- Must have First_Name
  AND (Last_Name IS NOT NULL AND TRIM(Last_Name) != '');  -- Must have Last_Name

SELECT * FROM employees_cleaned;

DESCRIBE employees_cleaned;

-- Add primary key and indexes

ALTER TABLE employees_cleaned 
MODIFY Employee_ID VARCHAR(50) NOT NULL;

ALTER TABLE employees_cleaned
ADD PRIMARY KEY (Employee_ID);

ALTER TABLE employees_cleaned 
MODIFY Department_Region VARCHAR(100);

ALTER TABLE employees_cleaned
ADD INDEX idx_department (Department_Region);

ALTER TABLE employees_cleaned 
MODIFY Status VARCHAR(50);

ALTER TABLE employees_cleaned
ADD INDEX idx_status (Status);

ALTER TABLE employees_cleaned
ADD INDEX idx_join_date (Join_Date);

ALTER TABLE employees_cleaned 
MODIFY Email VARCHAR(50);

ALTER TABLE employees_cleaned
ADD INDEX idx_email (Email);

-- Verify the cleaned table
SELECT 
    'Original Table' AS table_name,
    COUNT(*) AS row_count 
FROM messy_employee_table
UNION ALL
SELECT 
    'Cleaned Table',
    COUNT(*) 
FROM employees_cleaned;

-- Extra: Create a view for derived data

-- Create a view for derived data 
CREATE VIEW employee_derived_view AS
SELECT 
    e.Employee_ID,
    CONCAT(e.First_Name, ' ', e.Last_Name) AS full_name,
    CASE 
        WHEN e.Join_Date IS NOT NULL 
        THEN TIMESTAMPDIFF(YEAR, e.Join_Date, CURDATE())
        ELSE NULL
    END AS years_of_service,
    CASE 
        WHEN e.Age IS NULL THEN 'Unknown'
        WHEN e.Age < 25 THEN '18-24'
        WHEN e.Age BETWEEN 25 AND 34 THEN '25-34'
        WHEN e.Age BETWEEN 35 AND 44 THEN '35-44'
        WHEN e.Age BETWEEN 45 AND 54 THEN '45-54'
        WHEN e.Age BETWEEN 55 AND 64 THEN '55-64'
        WHEN e.Age >= 65 THEN '65+'
    END AS age_group,
    CASE 
        WHEN e.Salary IS NULL THEN 'Unknown'
        WHEN e.Salary < 50000 THEN 'Under $50K'
        WHEN e.Salary BETWEEN 50000 AND 74999 THEN '$50K-$75K'
        WHEN e.Salary BETWEEN 75000 AND 99999 THEN '$75K-$100K'
        WHEN e.Salary BETWEEN 100000 AND 149999 THEN '$100K-$150K'
        WHEN e.Salary >= 150000 THEN '$150K+'
    END AS salary_band,
    CASE 
        WHEN e.Email IS NOT NULL AND e.Email LIKE '%@%'
        THEN LOWER(SUBSTRING_INDEX(e.Email, '@', -1))
        ELSE NULL
    END AS email_domain,
    CASE 
        WHEN e.Join_Date IS NULL THEN 'Unknown'
        WHEN TIMESTAMPDIFF(YEAR, e.Join_Date, CURDATE()) < 1 THEN 'Less than 1 year'
        WHEN TIMESTAMPDIFF(YEAR, e.Join_Date, CURDATE()) BETWEEN 1 AND 2 THEN '1-2 years'
        WHEN TIMESTAMPDIFF(YEAR, e.Join_Date, CURDATE()) BETWEEN 3 AND 5 THEN '3-5 years'
        WHEN TIMESTAMPDIFF(YEAR, e.Join_Date, CURDATE()) BETWEEN 6 AND 10 THEN '6-10 years'
        WHEN TIMESTAMPDIFF(YEAR, e.Join_Date, CURDATE()) > 10 THEN 'Over 10 years'
    END AS tenure_category
FROM employees_cleaned e;

SELECT * FROM employee_derived_view;

SELECT * FROM employees_cleaned;

-- Thank you for viewing my project!!
































