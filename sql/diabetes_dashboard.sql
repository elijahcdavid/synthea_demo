with active_patients as
(
	select distinct *
	from public.patient as patient 
	where patient.deceased_date is null
),
diabetes_diagnosis as
(
	select condition.patient, min(onset_datetime) as earliest_diabetes_diagnosis
	from public.condition
	where code = '44054006'
	group by condition.patient
)

select patient.id,
	patient.birth_date,
	patient.deceased_date,
	patient.first_name,
	patient.last_name,
	patient.gender,
	patient.race,
	patient.zip,
	diabetes.earliest_diabetes_diagnosis,
	1 as diabetes_diagnosis
from public.patient as patient

join active_patients as active 
	on patient.id = active.id
inner join diabetes_diagnosis as diabetes
	on patient.id = diabetes.patient