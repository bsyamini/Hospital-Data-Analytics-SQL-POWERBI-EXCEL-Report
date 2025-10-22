create database Hospital_Records

use Hospital_Records
/*
--Table creation
create table patients_data (
    name VARCHAR(100) NOT NULL,
    age INT NOT NULL,
    gender VARCHAR(10) NOT NULL,
    blood_type VARCHAR(5) NOT NULL,
    medical_condition VARCHAR(50) NOT NULL,
    admission_date DATE NOT NULL,
    doctor VARCHAR(100) NOT NULL,
    hospital VARCHAR(100) NOT NULL,
    insurance_provider VARCHAR(50) NOT NULL,
    billing_amount DECIMAL(10,2) NOT NULL,
    room_number INT NOT NULL,
    admission_type VARCHAR(20) NOT NULL,
    discharge_date DATE NOT NULL,
    medication VARCHAR(50) NOT NULL,
    test_results VARCHAR(20) NOT NULL
)

--Inserting a table having 55500 rows in bulk
BULK INSERT patients_data
FROM 'C:\Data\healthcare_dataset.csv'
WITH (
    FORMAT = 'CSV',
    FIRSTROW = 2, -- Skip header row
    FIELDTERMINATOR = ',',  -- Column separator
    ROWTERMINATOR = '\n',   -- Row separator
    TABLOCK
)

*/
select top 10 * from patients_data

--Basic Data Exploration

--1.First 20 patients admitted.
select top 20 * from patients_data
order by admission_date 

--2.All patients admitted to a specific hospital.
select * from patients_data
where hospital = 'Sons And Miller'

--3.Names & medical conditions of patients older than 60.
select 
        name,
        medical_condition,
        age
from patients_data where age >60

--4.All patients admitted in January 2023.
select * from patients_data
where admission_date between '2023-01-01' and '2023-01-31'
order by admission_date

--5.All unique blood types.
select distinct blood_type from patients_data

--6.Details of patients treated by a specific doctor.
select * from patients_data 
where doctor ='Daniel Johnson'

--7.All unique medical conditions.
select distinct medical_condition from patients_data

--Aggregations & Grouping

--8.Patients count by gender.
select 
        gender,
        COUNT(*) as Total
from patients_data
    group by gender

--9.Average billing per hospital.
select 
        hospital,
        AVG(billing_amount) as Avg_Billing
from patients_data
    group by hospital
    order by Avg_Billing desc

--10.Total billing per insurance provider.
select 
        insurance_provider,
        SUM(billing_amount) as Total_Billing
from patients_data
    group by insurance_provider

--11.Maximum, Minimum, and Average number of days spent in hospital.
select  hospital,
        AVG(DATEDIFF(day, admission_date, discharge_date)) as Avg_Days,
        MIN(DATEDIFF(day, admission_date, discharge_date)) as Minimum_Days,
        MAX(CAST(DATEDIFF(day, admission_date, discharge_date) as float)) as Maximum_Days
from patients_data
    group by hospital

--12.Count admissions by admission type.
select 
        admission_type,
        COUNT(*) as Total_Admissions
from patients_data
    group by admission_type

--Date/Time Analysis

--13.Month with the most admissions
select  top 1
        DATEPART(month, admission_date) as Month_no,
        DATENAME(month,admission_date) as Month_Name,
        COUNT(*) as Total
from patients_data
    group by DATEPART(month, admission_date), DATENAME(month,admission_date)
    order by Total desc

--14.Year with highest revenue?
select 
        DATEPART(year, discharge_date) as Year,
        COUNT(*) as Total
from patients_data
    group by DATEPART(year, discharge_date)
    order by Total desc

--15.Average length of stay per year.
select 
        DATEPART(year, discharge_date) as Year,
        AVG(DATEDIFF(day, admission_date, discharge_date)) as Avg_Stay
from patients_data
    group by DATEPART(year, discharge_date)
    order by Avg_Stay DESC

--16.Count of admissions & discharges per week.
--Admissions per week
select 
        DATEPART(year, admission_date) as Year,
        DATENAME(week, admission_date) as Week_No,
        COUNT(*) as Total_Admissions
from patients_data
    group by DATEPART(year, admission_date),
    DATENAME(week, admission_date)
    order by  DATEPART(year, admission_date) ASC,
    DATENAME(week, admission_date) ASC

--Discharges per Week
select
        DATEPART(year,discharge_date) as Year,
        DATENAME(week,discharge_date) as Week_No,
        COUNT(*) as Total_Discharges
from patients_data
    group by DATEPART(year,discharge_date),
             DATENAME(week,discharge_date)
    order by DATEPART(year,discharge_date),
             DATENAME(week,discharge_date)


--if only weekname required for whole data
select
        DATEPART(weekday, admission_date) as Week_No,
        DATENAME(weekday,admission_date) as Week_Name,
        count(*) as Total
from patients_data
    group by DATEPART(weekday, admission_date),
             DATENAME(weekday,admission_date)
    order by DATEPART(weekday, admission_date);

--Below code will give both the tables in one table
with cteA as (
    select 
            DATEPART(weekday, admission_date) as Week_No,
            DATENAME(weekday, admission_date) as Week,
            COUNT(*) as Total_Admissions
from patients_data
    group by  DATEPART(weekday, admission_date),
              DATENAME(weekday, admission_date)
),
cteD as (
    select
            DATEPART(weekday, discharge_date) as Week_No,
            DATENAME(weekday, discharge_date) as Week,
            COUNT(*) as Total_Discharges
from patients_data
    group by DATEPART(weekday, discharge_date),
             DATENAME(weekday, discharge_date)
)
select 
        a.Week, a.Total_Admissions, d.Total_Discharges
        from cteA a join cteD d on a.Week = d.Week
        order by d.Week_No

--17.Find the busiest admission week of the year.
select Year,Week, Total from 
(
select  
        DATEPART(year, admission_date) as Year,
        DATENAME(week, admission_date) as Week,
        COUNT(*) as Total,
        ROW_NUMBER() over(partition by DATEPART(year, admission_date) 
        order by COUNT(*) desc) as rn
from patients_data
    group by DATEPART(year, admission_date),
             DATENAME(week, admission_date)
) as Sub 
where rn = 1
order by Total DESC;


--18.Find the difference in admissions between weekdays & weekends.
with PatientCount as
(
select 
    case
        when DATENAME(weekday, admission_date) in ('Saturday','Sunday') then 'Weekend'
        else 'WeekDay'
    end as Week_Day,
    COUNT(*) as Total_Patients
    from patients_data
    group by 
    case
        when DATENAME(weekday, admission_date) in ('Saturday','Sunday') then 'Weekend'
        else 'WeekDay'
    end 
)
select
        MAX(case when Week_Day = 'WeekDay' then Total_Patients end) as WeeDay_Count,
        MAX(case when Week_Day = 'Weekend' then Total_Patients end) as Weekend_Count,
        ABS( 
             MAX(case when Week_Day = 'WeekDay' then Total_Patients end) -
             MAX(case when Week_Day = 'Weekend' then Total_Patients end)
           ) as Difference
from PatientCount

--19.Patients with stay > 15 days.
select 
        name,
        DATEDIFF(day, admission_date, discharge_date) as No_of_days
from patients_data
    where  DATEDIFF(day, admission_date, discharge_date) >15
    order by No_of_days desc

--20.Seasonal trends (admissions across months).
select
        DATEPART(month, admission_date) as Month_No,
        DATENAME(month,admission_date) as Admission_Month,
        COUNT(*) as Total_Admissions
from patients_data
    group by DATEPART(month, admission_date), DATENAME(month,admission_date)
    order by DATEPART(month, admission_date) 

--Advanced Joins

--21.Joining Doctors & Patients → number of patients per doctor.
--To know the patients name treated by doctor
select
        d.doctor,
        p.name,
        COUNT(p.name) as Total
from patients_data p left join patients_data d
on p.hospital = d.hospital
group by d.doctor,
         p.name
order by d.doctor, Total desc

--To know the count of patients treatd by doctor
select doctor,
        COUNT(*) as Total_Patients
from patients_data
    group by doctor
    order by Total_Patients desc

--22.Joining Hospitals & Patients → revenue per hospital.
select 
        hospital,
        COUNT(*) as Total_Patients,
        FORMAT( SUM(billing_amount), 'c') as Revenue
-- or concat('$ ', SUM(billing_amount)) as Revenue
from patients_data
    group by hospital

--23.Joining Insurance Providers & Patients → claim amounts per provider.
select 
        insurance_provider,
        COUNT(*) as Total_patients,
        FORMAT(SUM(billing_amount),'c') as Claimed_Amount_by_Provider
from patients_data
    group by insurance_provider
    order by Claimed_Amount_by_Provider desc

--Window Functions
--24.Patients Rank by billing amount (overall).
select 
        name,
        billing_amount,
        RANK() over(partition by name order by billing_amount desc) 
        as Rank_of_patients
from patients_data
    order by billing_amount desc

--25.give all patients with refund/discount
select
        name,
        billing_amount
from patients_data
    where billing_amount <0
    order by billing_amount 

--Total refunded amount/discount amount
select 
        COUNT(*) as Total,
        SUM(billing_amount) as Total_Refunds
from patients_data
    where billing_amount<0

--26.Rank patients by billing amount per hospital.
select
        name,
        hospital,
        billing_amount,
        RANK() over(partition by hospital order by billing_amount desc) as Rank_on_Billing
from patients_data;
    
--27.Top 3 highest billing patients per hospital.
with cteB as 
        (
        select
            name,
            hospital,
            billing_amount,
            ROW_NUMBER() over(PARTITION by hospital order by billing_amount) 
        as Rank_of_Patients
        from patients_data
) 
    select top 3 
           name,
           hospital,
           billing_amount
    from cteB
    where Rank_of_Patients <= 3
    order by billing_amount desc;

--28.Running total of admissions per month.
--Running total for months of all years
with monthly_total as 
(
        select
        MONTH(admission_date) as Month_number,
        DATENAME(month,admission_date) as Month,
        COUNT(*) as Total
from patients_data
    group by MONTH(admission_date), DATENAME(MONTH,admission_date)
)
select 
       Month_number,
       Month,
       Total,
       SUM(Total) over(order by Month_number asc
       rows between unbounded preceding and current row) as Running_Sum_Monthly
from monthly_total
    group by Month_number,Month, Total
    order by Month_number, Running_Sum_Monthly;

--Running total for allmonths year wise
with Monthly_Report as
(
select
        YEAR(admission_date) as Year,
        MONTH(admission_date) as Month_Num,
        COUNT(*) as Total
from patients_data
    group by YEAR(admission_date),MONTH(admission_date)
)
select YEAR,
       Month_Num,
       Total,
       SUM(Total) over(partition by Year order by Month_Num
       rows between unbounded preceding and current row) as Running_Total
from Monthly_Report;

--29.Percentage contribution of each insurance provider to total billing.
with Percent_Contri as
(
select 
        insurance_provider,
        SUM(billing_amount) as Total_Amount
from patients_data
    group by  insurance_provider
)
select 
        insurance_provider,
        Total_Amount,
        CONCAT(CAST(
            Total_Amount * 100.0 / 
            (select SUM(billing_amount) from patients_data) as decimal(5,2)), '%') 
        as Percents
from Percent_Contri
    group by insurance_provider,Total_Amount
    order by Percents desc;

--Business Insights

--30.Hospital with the highest turnover (Admissions vs Discharges)?
with Turnover as 
(
select
        hospital,
        COUNT(admission_date) as Admissions,
        COUNT(discharge_date) as Discharges
from patients_data
    where admission_date is not null and discharge_date is not null
    group by hospital
)
select 
        hospital,
        Admissions,
        Discharges,
        Discharges - Admissions as Net_Diff
from Turnover
    order by Net_Diff 

--31.Doctor with the highest revenue
select  top 1
        doctor,
        COUNT(*) as All_Patients,
        SUM(billing_amount) as Total_Amount
from patients_data
    group by doctor
    order by Total_Amount desc

--32.Most common medical condition.
select  top 1
        medical_condition,
        COUNT(*) as Total_Patients
from patients_data
    group by medical_condition
    order by Total_Patients desc

--33.Gender that spends more time in hospital on average.
select
        gender,
        AVG(DATEDIFF(day,admission_date, discharge_date)) as Time_in_Hospital
from patients_data
    group by gender

--34.Most prescribed medication.
select top 1
       medication,
       COUNT(*) as Total
from patients_data
    group by medication

--35.Blood type with highest average billing.
select top 1
       blood_type,
       COUNT(*) as Total_Patients,
       FORMAT(AVG(billing_amount), 'C') as Avg_Billing
from patients_data
    group by blood_type
    order by Avg_Billing desc

--36.Insurance provider with longest patient stays.
select 
        insurance_provider,
        AVG(DATEDIFF(day,admission_date,discharge_date)) as Longest_Days
from patients_data
    group by insurance_provider
    order by Longest_Days desc

--37.Hospital with the most “Emergency” cases.
select  top 1
        hospital,
        admission_type,
        COUNT(*) as Total
from patients_data
    where admission_type = 'Emergency'
    group by hospital,
             admission_type
    order by Total desc;

--38.Patients admitted multiple times in the same year.
with Patients as
(
select 
        YEAR(admission_date) as Year,
        name,
        COUNT(name) as Total
from patients_data
    group by YEAR(admission_date), name
)
select 
        Year,
        name,
        Total
from Patients 
    where Total>1
    group by  Year,name, Total
    order by Total desc
