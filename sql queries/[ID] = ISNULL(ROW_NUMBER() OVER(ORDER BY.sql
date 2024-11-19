[ID] = ISNULL(ROW_NUMBER() OVER(ORDER BY (SELECT 1)), 0),
'CAADVAN' as [SOURCE_DATA],
'adv' as [SOURCE_SYSTEM],
[GLACode] = GLED.GLETCODE,
[GLADescription] = GLA.GLADESC,
[GeneralLedgerAccount] = GLS.GLSCODE + ' - ' + GLA.GLADESC,
[ReportType] = CASE WHEN GLA.GLATYPE = 15 THEN 2
	WHEN GLA.GLATYPE = 16 THEN 2
	WHEN GLA.GLATYPE = 9 THEN 2
	WHEN GLA.GLATYPE = 14 THEN 2
	WHEN GLA.GLATYPE = 13 THEN 2
	WHEN GLA.GLATYPE = 8 THEN 2
	WHEN GLA.GLATYPE = 20 THEN 1
	WHEN GLA.GLATYPE = 4 THEN 1
	WHEN GLA.GLATYPE = 5 THEN 1
	WHEN GLA.GLATYPE = 3 THEN 1
	WHEN GLA.GLATYPE = 2 THEN 1
	WHEN GLA.GLATYPE = 1 THEN 1 END,
[Type] = CASE WHEN GLA.GLATYPE = 15 THEN 'Expense - Other'
	WHEN GLA.GLATYPE = 16 THEN 'Expense - Taxes'
	WHEN GLA.GLATYPE = 9 THEN 'Income - Other'
	WHEN GLA.GLATYPE = 14 THEN 'Expense - Operating'
	WHEN GLA.GLATYPE = 13 THEN 'Expense - COS'
	WHEN GLA.GLATYPE = 8 THEN 'Income'
	WHEN GLA.GLATYPE = 20 THEN 'Equity'
	WHEN GLA.GLATYPE = 4 THEN 'Non-Current Liability'
	WHEN GLA.GLATYPE = 5 THEN 'Current Liability'
	WHEN GLA.GLATYPE = 3 THEN 'Fixed Asset'
	WHEN GLA.GLATYPE = 2 THEN 'Current Asset'
	WHEN GLA.GLATYPE = 1 THEN 'Non-Current Asset' END,
[TypeOrder] = CASE WHEN GLA.GLATYPE = 15 THEN 11
	WHEN GLA.GLATYPE = 16 THEN 12
	WHEN GLA.GLATYPE = 9 THEN 10
	WHEN GLA.GLATYPE = 14 THEN 9
	WHEN GLA.GLATYPE = 13 THEN 8
	WHEN GLA.GLATYPE = 8 THEN 7
	WHEN GLA.GLATYPE = 20 THEN 6
	WHEN GLA.GLATYPE = 4 THEN 5
	WHEN GLA.GLATYPE = 5 THEN 4
	WHEN GLA.GLATYPE = 3 THEN 3
	WHEN GLA.GLATYPE = 2 THEN 1
	WHEN GLA.GLATYPE = 1 THEN 2 END,
[Category] = CASE WHEN GLA.GLATYPE = 15 THEN 'Expenses'
  WHEN GLA.GLATYPE = 16 THEN 'Income Before Income Taxes'
  WHEN GLA.GLATYPE = 9 THEN 'Income From Operations'
  WHEN GLA.GLATYPE = 14 THEN 'Expenses'
  WHEN GLA.GLATYPE = 13 THEN 'Revenue'
  WHEN GLA.GLATYPE = 8 THEN 'Revenue'
  WHEN GLA.GLATYPE = 20 THEN 'Equity'
  WHEN GLA.GLATYPE = 4 THEN 'Liabilities'
  WHEN GLA.GLATYPE = 5 THEN 'Liabilities'
  WHEN GLA.GLATYPE = 3 THEN 'Assets'
  WHEN GLA.GLATYPE = 2 THEN 'Assets'
  WHEN GLA.GLATYPE = 1 THEN 'Assets' END,
[CategoryOrder] = CASE WHEN GLA.GLATYPE = 15 THEN 5
	WHEN GLA.GLATYPE = 16 THEN 7
	WHEN GLA.GLATYPE = 9 THEN 6
	WHEN GLA.GLATYPE = 14 THEN 5
	WHEN GLA.GLATYPE = 13 THEN 4
	WHEN GLA.GLATYPE = 8 THEN 4
	WHEN GLA.GLATYPE = 20 THEN 3
	WHEN GLA.GLATYPE = 4 THEN 2
	WHEN GLA.GLATYPE = 5 THEN 2
	WHEN GLA.GLATYPE = 3 THEN 1
	WHEN GLA.GLATYPE = 2 THEN 1
	WHEN GLA.GLATYPE = 1 THEN 1 END,
[PostPeriodCode] = GLS.GLSPP,
[DepartmentTeamCode] = DT.DP_TM_CODE,
[DepartmentTeamDescription] = DT.DP_TM_DESC,
[DepartmentTeam] = CASE WHEN DT.DP_TM_CODE IS NULL THEN NULL ELSE DT.DP_TM_CODE + ' - ' + DT.DP_TM_DESC END,
[OfficeCode] = O.OFFICE_CODE,
[OfficeName] = O.OFFICE_NAME,
[Office] = CASE WHEN O.OFFICE_CODE IS NULL THEN NULL ELSE O.OFFICE_CODE + ' - ' + O.OFFICE_NAME END,
[TransactionID] = GLED.GLETXACT,
[Description] = GLEH.GLEHDESC,
[TransactionDate] = GLEH.GLEHENTDATE,
[SourceDocument] = CASE WHEN GLEH.GLEHSOURCE IS NOT NULL THEN GLSR.GLSRCODE + ' - ' + GLSR.GLSRDESC ELSE NULL END,
[CreatedBy] = GLEH.GLEHUSER,
[SequenceNumber] = GLED.GLETSEQ,
[Reference] = GLED.GLETREM,
[Credit] = CASE WHEN GLED.GLETAMT < 0 THEN GLED.GLETAMT ELSE 0 END,
[Debit] = CASE WHEN GLED.GLETAMT > 0 THEN GLED.GLETAMT ELSE 0 END,
[Total] = GLED.GLETAMT,
[Quantity] = GLED.GLETQTY,
[PostPeriodCredit] = GLS.GLSCREDIT,
[PostPeriodDebit] = GLS.GLSDEBIT,
[PostPeriodTotal] = CAST((GLS.GLSCREDIT + GLS.GLSDEBIT) AS decimal(18,2)),
[PostPeriodQuantity] = GLS.GLSQTY,
[Budget1] = GLS.GLSBUDGET,
[Budget2] = GLS.GLSBUD2,
[Budget3] = GLS.GLSBUD3,
[GLEHMODDATE]