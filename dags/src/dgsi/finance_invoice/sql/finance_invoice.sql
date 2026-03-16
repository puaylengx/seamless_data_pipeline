WITH InvoiceData AS (
    SELECT DISTINCT
        inv.Id                   AS invoiceId,
        t.AcademicYear           AS acaYear,
        t.AcademicTerm           AS semester,
        inv.Number               AS invoiceNo,
        CASE
            WHEN inv.Type = 'r'         THEN 'N'
            WHEN inv.Type IN ('a','cr') THEN 'A'
            WHEN inv.Type = 'au'        THEN 'E'
            ELSE NULL
        END AS regisType,
        CASE
            WHEN inv.TotalAmount = 0
             AND nxt.Amount < 0
             AND nxt.TotalAmount < 0
             AND ABS(inv.Amount) > ABS(nxt.TotalAmount)
                THEN inv.Amount
            ELSE inv.TotalAmount
        END AS invoiceAmount,
        CASE
            WHEN inv.TotalAmount = 0
             AND nxt.Amount < 0
             AND nxt.TotalAmount < 0
             AND ABS(inv.Amount) > ABS(nxt.TotalAmount)
                THEN CONVERT(varchar(19), nxt.UpdatedAt, 120)
            WHEN r.Id IS NOT NULL AND inv.IsPaid = 1
                THEN CONVERT(varchar(19), inv.UpdatedAt, 120)
            WHEN t.AcademicYear = 2021
             AND t.AcademicTerm = 1
             AND ii.FeeItemName LIKE N'Lump sum%%'
                THEN CONVERT(varchar(19), inv.UpdatedAt, 120)
            WHEN inv.Number = '22009824'
                THEN CONVERT(varchar(19), inv.UpdatedAt, 120)
            ELSE NULL
        END AS paidDate,
        CASE
            WHEN (inv.TotalAmount = 0
             AND nxt.Amount < 0
             AND nxt.TotalAmount < 0
             AND ABS(inv.Amount) > ABS(nxt.TotalAmount))
             OR r.Id IS NOT NULL
                THEN CASE WHEN inv.TotalAmount = 0 THEN inv.Amount ELSE inv.TotalAmount END
            WHEN t.AcademicYear = 2021
             AND t.AcademicTerm = 1
             AND ii.FeeItemName LIKE N'Lump sum%%'
                THEN inv.TotalAmount
            WHEN inv.Number = '22009824'
                THEN inv.TotalAmount
            ELSE 0.00
        END AS paidAmount,
        CASE
            WHEN (inv.TotalAmount = 0
             AND nxt.Amount < 0
             AND nxt.TotalAmount < 0
             AND ABS(inv.Amount) > ABS(nxt.TotalAmount))
             OR r.Id IS NOT NULL
                THEN 'Y'
            WHEN t.AcademicYear = 2021
             AND t.AcademicTerm = 1
             AND ii.FeeItemName LIKE N'Lump sum%%'
                THEN 'Y'
            WHEN inv.Number = '22009824'
                THEN 'Y'
            ELSE 'N'
        END AS paidStatus,
        ii.FeeItemName,
        CASE WHEN ii.FeeItemName LIKE N'Lump sum%%' THEN N'เหมาจ่าย' ELSE N'หน่วยกิต' END AS invoiceType,
        NULL AS schNameTh,
        NULL AS remark,
        std.Code AS studentCode,
        ROW_NUMBER() OVER (
            PARTITION BY inv.Id
            ORDER BY CASE WHEN (CASE WHEN ii.FeeItemName LIKE N'Lump sum%%' THEN N'เหมาจ่าย' ELSE N'หน่วยกิต' END) = N'หน่วยกิต' THEN 0 ELSE 1 END
        ) AS rn
    FROM fee.Invoices inv
        INNER JOIN dbo.Terms t ON inv.TermId = t.Id
        INNER JOIN student.Students std ON inv.StudentId = std.Id
        LEFT JOIN fee.InvoiceItems ii ON ii.InvoiceId = inv.Id
        LEFT JOIN fee.Receipts r ON r.InvoiceId = inv.Id
    OUTER APPLY (
        SELECT TOP 1 inv2.Amount, inv2.TotalAmount, inv2.UpdatedAt
        FROM fee.Invoices inv2
        WHERE inv2.StudentId   = inv.StudentId
          AND inv2.Id          > inv.Id
          AND inv2.TotalAmount < 0
          AND inv2.Amount      < 0
          AND inv2.IsActive    = 1
          AND inv2.IsCancel    = 0
        ORDER BY inv2.Id
    ) nxt
    WHERE inv.IsActive = 1
      AND inv.IsCancel = 0
      AND NOT (inv.TotalAmount = 0 AND inv.IsPaid = 0)
      AND NOT inv.Number = 21000006
)
SELECT
    invoiceId, acaYear, semester, invoiceNo, regisType,
    invoiceAmount, paidDate, paidAmount, paidStatus,
    CASE WHEN FeeItemName LIKE N'Lump sum%%' THEN N'เหมาจ่าย' ELSE N'หน่วยกิต' END AS invoiceType,
    schNameTh, remark, studentCode
FROM InvoiceData
WHERE rn = 1 AND invoiceAmount > 0
ORDER BY studentCode, acaYear, semester;