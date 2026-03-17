select distinct
    std.Code code,
    title.NameEn as title,
    UPPER(LEFT(std.FirstNameEn, 1)) + LOWER(SUBSTRING(std.FirstNameEn, 2, LEN(std.FirstNameEn))) as first_name_en,
    UPPER(LEFT(std.MidNameEn, 1)) + LOWER(SUBSTRING(std.MidNameEn, 2, LEN(std.MidNameEn))) as middle_name_en,
    UPPER(LEFT(std.LastNameEn, 1)) + LOWER(SUBSTRING(std.LastNameEn, 2, LEN(std.LastNameEn))) as last_name_en,
    case when std.Gender = 1 then 'male'
          when std.Gender = 2 then 'female'
          else 'not specified'
     end as gender,
    case when nationality.NameEn = 'Myanmar' then 'Burmese'
          when nationality.NameEn = 'Myanmarian' then 'Burmese'
          when nationality.NameEn = 'Other' then 'Not Specified'
          when nationality.NameEn = 'TO_CONFIRM' then 'Not Specified'
          when nationality.NameEn = 'passed_all_required_courses' then 'Not Specified'
          else nationality.NameEn 
     end as nationality,
    residentType.NameEn as resident_type,
    studentFeeType.NameEn as student_fee_type,
    term.AcademicYear as academic_year,
    term.AcademicTerm as academic_term,
    admissionType.NameTh as admission_type,
    case when admissionType.Id = 1 then 'Full-time' /* IC + Outbound*/
         when admissionType.Id = 2 then 'Exchange'  /*Inbound*/
         when admissionType.Id = 3 then 'Exchange'  /*Inbound*/
         when admissionType.Id = 4 then 'Exchange'  /*Inbound*/
         when admissionType.Id = 5 then 'Full-time' /* PC */
         when admissionType.Id = 6 then 'Full-time' /* PC */
         when admissionType.Id = 7 then 'Summer'
         else 'External'
     end as student_type,
    case when std.StudentStatus = 'prc'	then 'Passed all required course'
         when std.StudentStatus = 'pa'	then 'Passed away'
         when std.StudentStatus = 'rs'	then 'Resign'
         when std.StudentStatus = 'dm'	then 'Dismissed'
         when std.StudentStatus = 's'	then 'Studying'
         when std.StudentStatus = 'la'	then 'Leave of absence'
         when std.StudentStatus = 'ex'	then 'Exchange'
         when std.StudentStatus = 'g'	then 'Graduated'
         when std.StudentStatus = 'np'	then 'No report'
         when std.StudentStatus = 'd'	then 'Deleted'
         when std.StudentStatus = 'b'	then 'Blacklist'
         when std.StudentStatus = 'tr'	then 'Transferred to other university'
         when std.StudentStatus = 're'	then 'Reenter'
         when std.StudentStatus = 'ra'	then 'Re admission'
         else 'Unknown' 
     end as student_status,
    -- case when std.StudentStatus = 'prc'	then 'Inactive'
    --     when std.StudentStatus = 'pa'	then 'Inactive'
    --     when std.StudentStatus = 'rs'	then 'Inactive'
    --     when std.StudentStatus = 'dm'	then 'Inactive'
    --     when std.StudentStatus = 's'	     then 'Active'
    --     when std.StudentStatus = 'la'	then 'Active'
    --     when std.StudentStatus = 'ex'	then 'Active'
    --     when std.StudentStatus = 'g'	     then 'Inactive'
    --     when std.StudentStatus = 'np'	then 'Inactive'
    --     when std.StudentStatus = 'd'	     then 'Inactive'
    --     when std.StudentStatus = 'b'	     then 'Inactive'
    --     when std.StudentStatus = 'tr'	then 'Inactive'
    --     -- when std.StudentStatus = 're'	then 'reenter'
    --     -- when std.StudentStatus = 'ra'	then 're_admission'
    --      else 'Unknown' 
    -- end as student_status_2,
    SUBSTRING(stagingStudent.programCode,1,4) as major_code,
    major.NameEn as major_name,
    major.Division as division,
    major.DivisionName as division_name,
    case when std.IsActive = 'true' then '1'
         else '0'
     end as is_active
from student.Students std
    left join master.Titles title on std.TitleId = title.Id
    left join master.Nationalities nationality on std.NationalityId = nationality.Id
    left join master.ResidentTypes residentType on std.ResidentTypeId = residentType.Id
    left join master.StudentFeeTypes studentFeeType on std.StudentFeeTypeId = studentFeeType.Id
    left join student.AdmissionInformations admissionInfo on std.Id = admissionInfo.StudentId
    left join dbo.Terms term on admissionInfo.AdmissionTermId = term.Id
    left join master.AdmissionTypes admissionType on admissionInfo.AdmissionTypeId = admissionType.Id
    left join dbo.StagingStudent stagingStudent on std.Code = stagingStudent.studentCode
    -- left join (
    --     select top(1) * from dbo.ALLMajor order by Id DESC
    -- ) major on SUBSTRING(stagingStudent.programCode,1,4) = major.Major
OUTER APPLY
(
    SELECT TOP (1)
          am.*
     FROM dbo.ALLMajor AS am
     WHERE am.Major = SUBSTRING(stagingStudent.programCode, 1, 4)
     ORDER BY am.Id DESC
) AS major
where std.StudentStatus in ('dm','ex','g','la','np','prc','pa', 'rs','s')
and term.AcademicYear >= '2016'
ORDER BY academic_year, academic_term, code;
-- where std.StudentStatus in ('dm','ex','g','la','np','prc','pa', 'rs','s')
-- and term.AcademicYear >= '2016'
-- and Code = '6681018'
-- where std.StudentStatus in (/*__STATUSES__*/)
--   and term.AcademicYear >= %s
-- where std.StudentStatus in ({statuses})
--   and term.AcademicYear >= {min_academic_year}
-- order by academic_year, academic_term, code
