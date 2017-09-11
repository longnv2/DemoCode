select BRANCH_NAME, SUM(LCY_BAL)/1000000000 AS TOTAL
from FactDeposit
where COB_DATE = '20170615'
	and REGION = 'HCM'
	AND FLAG IS NULL
	AND SEGMENT = 'KHDN'
	AND BRANCH_NAME IN(
					 N'Chi Nhánh Hồ Chí Minh'
					, N'Chi Nhánh Gia Định'
					 , N'Chi Nhánh Hoàng Văn Thụ'
					, N'Chi Nhánh Sài Gòn'
					, N'Chi Nhánh Tân Bình'
					, N'Chi Nhánh Phú Nhuận'
					, N'Chi Nhánh Thủ Đức'
	)
	AND SUB_SEGMENT NOT LIKE N'%Khách hàng UPPER SME%'
	AND APP = 'KKH'

	--AND CONTRACT_NO = 'AA1711080GHK'
group by BRANCH_NAME
ORDER BY BRANCH_NAME

select  * from STG_T24..F_AA_ARRANGEMENT 
where ARRANGEMENT_ID = 'AA1711080GHK' 
order by DATA_DATETIME

AA17065G6S27
AA17128B0555
STG_T24.F_FUNDS_TRANSFER
SELECT * FROM FactDeposit WHERE COB_DATE= '20170619' AND CONTRACT_NO = 'AA1711080GHK'


select  * from STG_T24..F_AA_INTEREST_ACCRUALS 
where PROPERTY_NAME like 'AA17128B0555%' AND TO_DATE = DATA_DATETIME 
order by DATA_DATETIME


SELECT * FROM FactDeposit WHERE COB_DATE = '20170615' and CONTRACT_NO = 'AA1711080GHK'

select count(1) 
 from STG_T24..F_AA_INTEREST_ACCRUALS
where TO_DATE = '20170615' and DATA_DATETIME = '20170615'


select * from DimCustomer where CUSTOMER_CODE = '10396664'

select top 10 * from STG_T24..F_AA_ACCOUNT_DETAILS where ARRANGEMENT_ID = 'AA1711080GHK'
20000000000
20000000000


select * from LOG_DB..CHECK_DATA_DAILY where COB_DATE = '20170620'
select * from LOG_DB..DAILY_ETL_TABLE_LIST


select top 10 * from F_ACCOUNT



select *
from FactDepositSummary
where COB_DATE = '20170615'
and REGION = 'HCM'
AND FLAG IS NULL
AND BRANCH_NAME = N'Chi Nhánh Gia Định'
and APP = 'KKH'
AND SEGMENT = 'KHDN'

select  BRANCH_NAME,  SUM(LD_PR + PD_PR)/1000000000 AS TOTAL
from FactLoanSummary
where COB_DATE = '20170523'
and FLAG is null
and REGION = 'HCM'
AND SEGMENT = 'KHDN'
AND BRANCH_NAME IN(N'Chi Nhánh Gia Định'
				, N'Chi Nhánh Hồ Chí Minh'
				, N'Chi Nhánh Hoàng Văn Thụ'
				, N'Chi Nhánh Sài Gòn'
				, N'Chi Nhánh Tân Bình'
				, N'Chi Nhánh Phú Nhuận'
				, N'Chi Nhánh Thủ Đức'
)
GROUP BY BRANCH_NAME

[SSIS.Pipeline] Error: "F_CAMCOSTK_PVC" failed validation and returned validation status "VS_NEEDSNEWMETADATA".





	select top 10 *
	from F_LD_LOANS_DEPOSITS_KPIOWNER 
	where CONTRACT_NO_ = 'LD1704710040'
	

	select * from PVCom..FactLoan where CONTRACT_NO = 'LD1711708562'

	select * from PVCom..MasterLoan where CONTRACT_NO = 'LD1711708562'

	select top 10 * from F_LD_LOANS_AND_DEPOSITS where CONTRACT_NO_ = 'LD1711708562'

	select * from PVCom..DimBranch where BRANCH_CODE = 'VN0016350'
