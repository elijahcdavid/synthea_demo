with diabetes_diagnosis as
(
	select condition.patient, min(onset_datetime) as earliest_diabetes_diagnosis
	from public.condition
	where code = '44054006'
	group by condition.patient
)
SELECT patient.id,
	patient.birth_date,
	patient.first_name,
	patient.last_name,
	patient.gender,
	patient.race,
	diabetes.earliest_diabetes_diagnosis,
	case when diabetes.earliest_diabetes_diagnosis is not null then 1
		else 0
		end as diabetes_diagnosis
FROM public.patient as patient

left join diabetes_diagnosis as diabetes
	on patient.id = diabetes.patient

select * from public.patient