SELECT DISTINCT
    std.Code AS studentCode,
    CASE
        WHEN std.TitleId = '17' THEN '002'
        WHEN std.TitleId IN ('12','26','30') OR (std.TitleId = '44' AND std.Code IN ('5380028')) THEN '003'
        WHEN std.TitleId IN ('14','18') THEN '001'
        ELSE 'Not Specified'
    END AS prefixCode,
    CASE
        WHEN std.TitleId = '17' THEN 'Mrs.'
        WHEN std.TitleId IN ('12','26','30') OR (std.TitleId = '44' AND std.Code IN ('5380028')) THEN 'Ms.'
        WHEN std.TitleId IN ('14','18') THEN 'Mr.'
        ELSE 'Not Specified'
    END AS prefix,
    CASE WHEN std.FirstNameTh IS NULL OR TRIM(std.FirstNameTh) = '' THEN UPPER(TRIM(std.FirstNameEn))
            ELSE UPPER(TRIM(std.FirstNameTh)) END AS firstNameTh,
    CASE WHEN std.MidNameTh   IS NULL OR TRIM(std.MidNameTh)   = '' THEN UPPER(TRIM(std.MidNameEn))
            ELSE UPPER(TRIM(std.MidNameTh))   END AS middleNameTh,
    CASE WHEN std.LastNameTh  IS NULL OR TRIM(std.LastNameTh)  = '' THEN UPPER(TRIM(std.LastNameEn))
            ELSE UPPER(TRIM(std.LastNameTh))  END AS lastNameTh,
    UPPER(TRIM(std.FirstNameEn)) AS firstNameEn,
    UPPER(TRIM(std.MidNameEn))   AS middleNameEn,
    UPPER(TRIM(std.LastNameEn))  AS lastNameEn,
    CASE WHEN std.Gender = 1 THEN 'male'
            WHEN std.Gender = 2 OR std.Code IN ('5380028') THEN 'female'
            ELSE 'Not Specified' END AS genderName,
    term.AcademicYear  AS admitYear,
    term.AcademicTerm  AS intakeTermNumber,
    'IC' AS facultyName,
    curri.AbbreviationEn + '-001-B' AS programCode,
    curriVersion.NameEn AS programName,
    'major' AS programType,
    'FAC_01' AS entranceTypeName,
    CASE WHEN NationalityId = 93 THEN 'resident' ELSE 'non-resident' END AS residentType,
    NULL AS studentType,
    CASE
        WHEN curri.AbbreviationEn IN ('DTDS','PYPY') THEN 'take a course with IC'
        WHEN std.StudentStatus = 'prc' THEN 'passed_all_required_courses'
        WHEN std.StudentStatus = 'pa'  THEN 'passed_away'
        WHEN std.StudentStatus = 'rs'  THEN 'resigned'
        WHEN std.StudentStatus = 'dm'  THEN 'dismissed'
        WHEN std.StudentStatus = 's'   THEN 'studying'
        WHEN std.StudentStatus = 'la'  THEN 'leave_of_absence'
        WHEN std.StudentStatus = 'ex'  THEN 'exchange'
        WHEN std.StudentStatus = 'g'   THEN 'graduated'
        WHEN std.StudentStatus = 'g1'  THEN 'graduated_with_first_class_honors'
        WHEN std.StudentStatus = 'g2'  THEN 'graduated_with_second_class_honors'
        WHEN std.StudentStatus = 'np'  THEN 'no_report'
        WHEN std.StudentStatus = 'd'   THEN 'deleted'
        WHEN std.StudentStatus = 'b'   THEN 'blacklist'
        WHEN std.StudentStatus = 'tr'  THEN 'transferred_to_other_university'
        WHEN std.StudentStatus = 're'  THEN 'reenter'
        WHEN std.StudentStatus = 'ra'  THEN 're_admission'
        ELSE 'Others' END AS studentStatusName,
    FORMAT(SWITCHOFFSET(CONVERT(datetimeoffset, std.BirthDate), '+07:00'), N'dd/MM/yyyy','th-TH') AS birthdate,
    std.Email AS email,
    NULL AS NationalityName,
    NULL AS religionName,
    NULL AS bloodTypeName,
    NULL AS maritalStatusName,
    NULL AS numberOfSiblings,
    NULL AS sequenceChild,
    NULL AS numberOfSiblingsStillStudying,
    CASE WHEN std.CitizenNumber IS NULL OR TRIM(std.CitizenNumber) = '' THEN NULL ELSE std.CitizenNumber END AS citizenId,
    CASE WHEN std.Passport      IS NULL OR TRIM(std.Passport)      = '' THEN NULL ELSE std.Passport      END AS passportId,
    NULL AS passportStartDate,
    NULL AS passportEndDate,
    NULL AS passportStatusName,
    NULL AS raceName,
    YEAR(graduateInfo.GraduatedAt) AS graduateYear,
    NULL AS deformName,
    NULL AS deformCardId,
    NULL AS deformCardStartdate,
    NULL AS deformCardEnddate,
    NULL AS terminateStudyCause,
    NULL AS studyTypeName,
    NULL AS studyTimeName,
    CASE
        WHEN academicInfo.CreditEarned <= 36       THEN 1
        WHEN academicInfo.CreditEarned BETWEEN 37 AND 70  THEN 2
        WHEN academicInfo.CreditEarned BETWEEN 71 AND 105 THEN 3
        WHEN academicInfo.CreditEarned > 105       THEN 4
        ELSE '1'
    END AS class,
    NULL AS programRegistName,
    NULL AS gradStatusName,
    NULL AS dateGraduation,
    NULL AS talentName,
    NULL AS systemId
FROM student.Students AS std
LEFT JOIN student.CurriculumInformations curriInfo ON curriInfo.StudentId = std.Id
LEFT JOIN curriculum.CurriculumVersions  curriVersion ON curriVersion.Id = curriInfo.CurriculumVersionId
LEFT JOIN curriculum.Curriculums         curri ON curri.Id = curriVersion.CurriculumId
LEFT JOIN student.AcademicInformations   academicInfo ON academicInfo.StudentId = std.Id
LEFT JOIN student.AdmissionInformations  admissionInfo ON admissionInfo.StudentId = std.Id
LEFT JOIN dbo.Terms term ON term.Id = admissionInfo.AdmissionTermId
LEFT JOIN student.GraduationInformations graduateInfo on std.Id = graduateInfo.StudentId
WHERE std.Code >= '50%'
AND std.Code  < '90%'
AND curriInfo.IsActive = '1'
ORDER BY std.Code;