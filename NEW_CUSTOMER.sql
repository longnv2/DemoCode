select TOP 1000 * from DimCustomer

select * 
from INFORMATION_SCHEMA.COLUMNS 
where TABLE_NAME  = 'DimCustomer'


CREATE TABLE DimCustomer_NEW (
		CIF numeric,
		MNEMONIC varchar (12),
		CUSTOMER_NAME nvarchar (70),
		STREET nvarchar (50),
		ADDRESS nvarchar (80),
		TOWN nvarchar (80),
		COUNTRY nvarchar (30),
		SECTOR varchar (5),
		ACCOUNT_OFFICER nvarchar (4),
		INDUSTRY nvarchar (4),
		TARGET nvarchar (4),
		NATIONALITY nvarchar (2),
		CUSTOMER_STATUS nvarchar (4),
		RESIDENCE nvarchar (2),
		CONTACT_DATE DATE,
		LANGUAGE nvarchar (2),
		COMPANY_BOOK nvarchar (10),
		ASSET_CLASS nvarchar (3),
		TITLE nvarchar (50),
		GENDER nvarchar (10),
		DATE_OF_BIRTH DATE,
		MARITAL_STATUS nvarchar (11),
		FAX_1 nvarchar (20),
		CUSTOMER_TYPE nvarchar (50),
		INTERNET_BANKING_SERVICE nvarchar (4),
		MOBILE_BANKING_SERVICE nvarchar (4),
		OVERRIDE nvarchar (50),
		CURR_NO nvarchar (4),
		DATE_TIME DATETIME,
		CO_CODE nvarchar (11),
		DEPT_CODE nvarchar (4),
		PROVINCE nvarchar (70),
		CURR nvarchar (1),
		INSERT_DATE DATE,
		LEGAL_DOC_NAME varchar (8),
		LEGAL_HOLDER_NAME nvarchar (50),
		PHONE_NUMBER varchar (35),
		EMAIL varchar (40),
		LEGAL_ID varchar (21),
		EFFECTIVE_FROM_DATETIME DATE,
		EFFECTIVE_TO_DATETIME DATE
)


INSERT INTO DimCustomer_NEW(
		  CIF 
		, MNEMONIC 
		, CUSTOMER_NAME 
		, STREET 
		, ADDRESS 
		, TOWN 
		, COUNTRY 
		, SECTOR 
		, ACCOUNT_OFFICER 
		, INDUSTRY 
		, TARGET 
		, NATIONALITY 
		, CUSTOMER_STATUS 
		, RESIDENCE 
		, CONTACT_DATE 
		, LANGUAGE 
		, COMPANY_BOOK 
		, ASSET_CLASS 
		, TITLE 
		, GENDER 
		, DATE_OF_BIRTH 
		, MARITAL_STATUS 
		, FAX_1 
		, CUSTOMER_TYPE 
		, INTERNET_BANKING_SERVICE 
		, MOBILE_BANKING_SERVICE 
		, OVERRIDE 
		, CURR_NO 
		, DATE_TIME 
		, CO_CODE 
		, DEPT_CODE 
		, PROVINCE 
		, CURR 
		, INSERT_DATE 
		, LEGAL_DOC_NAME 
		, LEGAL_HOLDER_NAME 
		, PHONE_NUMBER 
		, EMAIL 
		, LEGAL_ID 
		, EFFECTIVE_FROM_DATETIME 
		, EFFECTIVE_TO_DATETIME ) 
SELECT CUSTOMER_CODE AS CIF
		, MNEMONIC 
		, CONCAT(ISNULL(NAME_1, ''), ISNULL(NAME_2, '')) AS CUSTOMER_NAME 
		, STREET 
		, ADDRESS 
		, TOWN_COUNTRY AS TOWN
		, COUNTRY 
		, SECTOR 
		, ACCOUNT_OFFICER 
		, INDUSTRY 
		, TARGET 
		, NATIONALITY 
		, CUSTOMER_STATUS 
		, RESIDENCE 
		, CONTACT_DATE 
		, LANGUAGE 
		, COMPANY_BOOK 
		, ASSET_CLASS 
		, TITLE 
		, GENDER 
		, DATE_OF_BIRTH 
		, MARITAL_STATUS 
		, FAX_1 
		, CUSTOMER_TYPE 
		, INTERNET_BANKING_SERVICE 
		, MOBILE_BANKING_SERVICE 
		, OVERRIDE 
		, CURR_NO 
		, DATE_TIME 
		, CO_CODE 
		, DEPT_CODE 
		, PROVINCE 
		, CURR 
		, INSERT_DATE 
		, LEGAL_DOC_NAME 
		, LEGAL_HOLDER_NAME 
		, PHONE_NUMBER 
		, EMAIL 
		, LEGAL_ID 
		, EFFECTIVE_FROM_DATETIME 
		, EFFECTIVE_TO_DATETIME 
FROM DimCustomer



