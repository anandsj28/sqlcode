USE [Test_Netting]
GO

/****** Object:  StoredProcedure [dbo].[usp_ImportFileImport_2]    Script Date: 13-12-2023 15:02:14 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



/*
select COUNT(1) from Inv
select COUNT(1) from Ins
select COUNT(1) from RefMain
select COUNT(1) from WHTInv
select COUNT(1) from RefContact
select COUNT(1) from ImportLine
select COUNT(1) from ImportFile
*/


-- =============================================
-- Author:		Macer
-- Create date: 20120606
-- Description:	New ImportFile Sp
-- Basically Imports data as follows
-- 1. References:
--    A. RefMain
--    B. RefAddr
--    C. RefContact
--       - Tax 1
--       - Tax 2
--       - Contact 
--       - Fax 1
--       - Fax 2
-- 2. Instructions
--    A. Ins
-- 3. Invoices
--    A. Inv
-- 4. WHT
-- =============================================
CREATE PROCEDURE [dbo].[usp_ImportFileImport_2] 
	-- Add the parameters for the stored procedure here
	@tnImpFileId INT
AS
BEGIN

   -- SET NOCOUNT ON added to prevent extra result sets from interfering with SELECT statements.
   -- NOCOUNT should be off so that we get a return value in .net (otherwise we get -1)
   SET NOCOUNT OFF;


   DECLARE @cSystemId varchar(16)
   SELECT @cSystemId = cSystemId FROM ImportFile WITH (NOLOCK) WHERE nImpFileId = @tnImpFileId

   -- .REFMAIN. --
   -- Create a RefMain record for each new reference...
   EXEC usp_UpdateProgress 'Importporting Reference Data. RefMain', 5


   -- The columns in ImportLine are not persisted, so the first thing we do is to create a temp working table to persist the data temporarily.
   SELECT cToRefCk    -- Payee
		 ,cToRefName    -- VendorName
		 ,cToRefAddr    -- VendorLocn (country code)
		 ,cToRefCnt    -- ContactNam
		 ,cToRefFax    -- FaxNumber
		 ,cToRefMail    -- EmailAddr
		 --,cToRefFax2    -- FaxNumber2
		 ,cRefTxCd1    -- Taxcode
		 ,cRefTxCd2    -- Taxcode2
		 ,cPancode    -- Pancode
		 ,cImpRecTyp    -- R = REF
		 ,nImpLineId
		 ,nStatus
		 ,nImpRecId
		 ,cCmpCk
		 ,cToRefStreet
		 ,cToRefPostal
		 ,cToRefCity
		 ,dbo.GetRefType(cToRefCk, @cSystemId) as cRefTyp
		 ,dbo.GetRefSystemId(cToRefCk, @cSystemId) as cSystemId
     INTO #ImportLineRef
     FROM ImportLine 
    WHERE nImpFileId = @tnImpFileId
      AND cImpRecTyp = 'R'

   CREATE INDEX IX_ImportLineRef_cToRefCk ON #ImportLineRef (cToRefCk)
   CREATE INDEX IX_ImportLineRef_cToRefCk_cRefTyp_cSystemId ON #ImportLineRef (cToRefCk, cRefTyp, cSystemId)
   CREATE INDEX IX_ImportLineRef_cToRefName_cToRefAddr ON #ImportLineRef (cToRefName, cToRefAddr)
   CREATE INDEX IX_ImportLineRef_nImpLineId ON #ImportLineRef (nImpLineId)
   

   -- update missing system on RefMain. 20190214. MKS.Only update if there is not already a target ref. --select * from RefMain where cRefCK = '0005282978' (Import File 43163
   MERGE RefMain AS Target
   USING (SELECT DISTINCT cToRefCk, cRefTyp, cSystemId FROM #ImportLineRef WHERE NOT EXISTS ( SELECT 1 FROM RefMain RM WITH (NOLOCK) WHERE RM.cRefCk = #ImportLineRef.cToRefCK AND RM.cSystemId = @cSystemId )) AS Source 
				(cToRefCk, cRefTyp, cSystemId)
   ON (Target.cRefCk = Source.cToRefCk AND isnull(Target.cSystemId, '') = '')
   WHEN MATCHED THEN
		UPDATE SET cSystemId = Source.cSystemId
   ;

   -- updaet system for companies, we receive companie codes from different ERP, but their system is always Netting
   MERGE RefMain AS Target
   USING (SELECT DISTINCT cToRefCk, cRefTyp, cSystemId FROM #ImportLineRef) AS Source 
				(cToRefCk, cRefTyp, cSystemId)
   ON (Target.cRefCk = Source.cToRefCk AND Source.cRefTyp = 'C' AND Source.cSystemId = 'Netting')
   WHEN MATCHED THEN
		UPDATE SET cSystemId = Source.cSystemId, cRefTyp = Source.cRefTyp
   ;

   -- update wrong RefTyp on RefMain and insert new records
   MERGE RefMain AS Target
   USING (SELECT DISTINCT cToRefCk, cRefTyp, cSystemId FROM #ImportLineRef) AS Source 
				(cToRefCk, cRefTyp, cSystemId)
   ON (Target.cRefCk = Source.cToRefCk and Target.cSystemId = Source.cSystemId)
   WHEN MATCHED AND isnull(Target.cRefTyp, '') <> Source.cRefTyp THEN 
		UPDATE SET cRefTyp=Source.cRefTyp
   WHEN NOT MATCHED AND LEN(cToRefCK) < 17 THEN	
		INSERT (cRefCk, cRefTyp, cSystemId)
		VALUES (Source.cToRefCk, Source.cRefTyp, Source.cSystemId)
   ;



   -- .REFAddress. --
   -- if record already exists, update the nImpRecId
   EXEC usp_UpdateProgress 'Importing Reference Data. RefAddress', 10

   -- update status of lines which are already present in system
   UPDATE #ImportLineRef
      SET nStatus = 3,
          nImpRecId = RA.nRefAddrId  
     FROM #ImportLineRef IL WITH (NOLOCK) 
     INNER JOIN RefMain RM ON IL.cToRefCK = RM.cRefCK AND RM.cSystemId = IL.cSystemId
     INNER JOIN RefAddress RA WITH (NOLOCK) ON RA.nRefId = RM.nRefId AND
                                               RA.cRefName = IL.cToRefName AND
                                               RA.cRefAddr = IL.cToRefAddr
    WHERE ISNULL( IL.nImpRecId, 0) = 0 AND IL.nStatus = 0;


   -- special handling for group companies, we don't overwrite ref address with what we receive from the ERP
   UPDATE #ImportLineRef
      SET nStatus = 3,
          nImpRecId = RA.nRefAddrId  
     FROM #ImportLineRef IL WITH (NOLOCK) 
     INNER JOIN RefMain RM ON IL.cToRefCK = RM.cRefCK AND RM.cSystemId = IL.cSystemId
     INNER JOIN RefAddress RA WITH (NOLOCK) ON RA.nRefId = RM.nRefId
    WHERE ISNULL( IL.nImpRecId, 0) = 0 AND IL.nStatus = 0
	  AND RM.cRefTyp = 'C' AND RM.cSystemId = 'Netting';

   
   -- insert new addresses
   INSERT INTO RefAddress ( nRefId, cRefName, cRefAddr)     
     SELECT DISTINCT RM.nRefId, IL.cToRefName, IL.cToRefAddr
       FROM #ImportLineRef IL WITH (NOLOCK) 
      INNER JOIN RefMain RM ON IL.cToRefCK = RM.cRefCK AND RM.cSystemId = IL.cSystemId
      WHERE ISNULL( IL.nImpRecId, 0) = 0 AND IL.nStatus = 0;

   -- update status on new insert addresses
   UPDATE IL
      SET nStatus = 4,
          nImpRecId = RA.nRefAddrId  
     FROM #ImportLineRef IL WITH (NOLOCK) 
     INNER JOIN RefMain RM ON IL.cToRefCK = RM.cRefCK AND RM.cSystemId = IL.cSystemId
     INNER JOIN RefAddress RA WITH (NOLOCK) ON RA.nRefId = RM.nRefId AND
                                               RA.cRefName = IL.cToRefName AND
                                               RA.cRefAddr = IL.cToRefAddr
     WHERE ISNULL( IL.nImpRecId, 0) = 0 AND IL.nStatus = 0;

   -- REFCONTACT
   EXEC usp_UpdateProgress 'Importing Reference Data. RefContact', 15

   -- remove any existing ImportRefContact Lines that reference these importlines.
   DELETE ImportRefContact
    WHERE EXISTS ( SELECT 1 
                     FROM #ImportLineRef IL WITH (NOLOCK)
                    WHERE IL.nImpLineId = ImportRefContact.nImpLineId)

   -- Insert any previously unseen contact information into refcontact
   -- TAX 1. (Type '4', field cRefTxCd1)
   INSERT INTO RefContact ( cContTyp, cContInf)
      SELECT '4', cRefTxCd1
         FROM #ImportLineRef IL WITH (NOLOCK)
        WHERE IL.cImpRecTyp = 'R' AND
              IL.cRefTxCd1 <> '' AND
              NOT EXISTS ( SELECT 1 FROM RefContact RC  WITH (NOLOCK) WHERE RC.cContTyp = '4' AND IL.cRefTxCd1 = RC.cContInf)

   INSERT INTO ImportRefContact ( nImpLineId, nContId, nSequence)
   SELECT IL.nImpLineId, RC.nContId, 0
     FROM #ImportLineRef IL WITH (NOLOCK) 
     JOIN RefContact RC WITH (NOLOCK) ON IL.cRefTxCd1 = RC.cContInf AND RC.cContTyp = '4'
    WHERE IL.cRefTxCd1 <> ''
          

   -- TAX 2. (Type '6', field cRefTxCd2)
   INSERT INTO RefContact ( cContTyp, cContInf)
      SELECT '6', cRefTxCd2
         FROM #ImportLineRef IL WITH (NOLOCK)
        WHERE IL.cRefTxCd2 <> '' AND
              NOT EXISTS ( SELECT 1 FROM RefContact RC  WITH (NOLOCK) WHERE RC.cContTyp = '6' AND IL.cRefTxCd2 = RC.cContInf)

   INSERT INTO ImportRefContact ( nImpLineId, nContId, nSequence)
   SELECT IL.nImpLineId, RC.nContId, 0
     FROM #ImportLineRef IL WITH (NOLOCK) 
     JOIN RefContact RC WITH (NOLOCK) ON IL.cRefTxCd2 = RC.cContInf AND RC.cContTyp = '6'
    WHERE IL.cRefTxCd2 <> ''

   -- Pancode. (Type '7', field cPancode)
   INSERT INTO RefContact ( cContTyp, cContInf)
      SELECT '7', cPancode
         FROM #ImportLineRef IL WITH (NOLOCK)
        WHERE IL.cPancode <> '' AND
              NOT EXISTS ( SELECT 1 FROM RefContact RC  WITH (NOLOCK) WHERE RC.cContTyp = '7' AND IL.cPancode = RC.cContInf)

   INSERT INTO ImportRefContact ( nImpLineId, nContId, nSequence)
   SELECT IL.nImpLineId, RC.nContId, 0
     FROM #ImportLineRef IL WITH (NOLOCK) 
     JOIN RefContact RC WITH (NOLOCK) ON IL.cPancode = RC.cContInf AND RC.cContTyp = '7'
    WHERE IL.cPancode <> ''
          
   -- CONTACT (Type '1', field cToRefCnt)
   INSERT INTO RefContact ( cContTyp, cContInf)
      SELECT '1', cToRefCnt
         FROM #ImportLineRef IL WITH (NOLOCK)
        WHERE IL.cToRefCnt <> '' AND
              NOT EXISTS ( SELECT 1 FROM RefContact RC  WITH (NOLOCK) WHERE RC.cContTyp = '1' AND IL.cToRefCnt = RC.cContInf)

   INSERT INTO ImportRefContact ( nImpLineId, nContId, nSequence)
   SELECT IL.nImpLineId, RC.nContId, 0
     FROM #ImportLineRef IL WITH (NOLOCK) 
     JOIN RefContact RC WITH (NOLOCK) ON IL.cToRefCnt = RC.cContInf AND RC.cContTyp = '1'
    WHERE IL.cToRefCnt <> ''

   -- FAX 1 ( Type '2', Field cToRefFax)
   INSERT INTO RefContact ( cContTyp, cContInf)
      SELECT '2', REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( cToRefFax, '-', ''), '/', ''), '\', ''), ' ', ''), ',', ''), ';', '')
         FROM #ImportLineRef IL WITH (NOLOCK)
        WHERE IL.cToRefFax <> '' AND
              IL.cToRefFax <> '0000' AND
              NOT EXISTS ( SELECT 1 FROM RefContact RC  WITH (NOLOCK) WHERE RC.cContTyp = '2' AND REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( IL.cToRefFax, '-', ''), '/', ''), '\', ''), ' ', ''), ',', ''), ';', '') = RC.cContInf)

   INSERT INTO ImportRefContact ( nImpLineId, nContId, nSequence)
   SELECT DISTINCT IL.nImpLineId, RC.nContId, 0
     FROM #ImportLineRef IL WITH (NOLOCK) 
     JOIN RefContact RC WITH (NOLOCK) ON REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( IL.cToRefFax, '-', ''), '/', ''), '\', ''), ' ', ''), ',', ''), ';', '') = RC.cContInf AND RC.cContTyp = '2'
    WHERE IL.cToRefFax <> ''  AND
          IL.cToRefFax <> '0000';

   -- we don't use fax 2, so don't import it
   -- FAX 2 ( Type '2', Field cToRefFax2 )
--   INSERT INTO RefContact ( cContTyp, cContInf)
--      SELECT '2', REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( cToRefFax2, '-', ''), '/', ''), '\', ''), ' ', ''), ',', ''), ';', '')
--         FROM #ImportLineRef IL WITH (NOLOCK)
--        WHERE IL.cToRefFax2 <> '' AND
--              IL.cToRefFax2 <> '0000' AND
--              NOT EXISTS ( SELECT 1 FROM RefContact RC  WITH (NOLOCK) WHERE RC.cContTyp = '2' AND REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( IL.cToRefFax2, '-', ''), '/', ''), '\', ''), ' ', ''), ',', ''), ';', '') = RC.cContInf)

--   INSERT INTO ImportRefContact ( nImpLineId, nContId, nSequence)
--   SELECT DISTINCT IL.nImpLineId, RC.nContId, 0
--     FROM #ImportLineRef IL WITH (NOLOCK) 
--     JOIN RefContact RC WITH (NOLOCK) ON REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( IL.cToRefFax2, '-', ''), '/', ''), '\', ''), ' ', ''), ',', ''), ';', '') = RC.cContInf AND RC.cContTyp = '2'
--    WHERE IL.cToRefFax2 <> ''  AND
--          IL.cToRefFax2 <> '0000';

   -- MAIL
   -- Create a temp table of the email addresses in the download file.
   WITH myCTE AS (
      SELECT nImpLineId,
             CAST('<I>' + REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( cToRefMail, 'SMTP:', ''), '¦', '|'), '&', '&amp;'), '<', '|'), '>', ''), '|', '</I><I>') + '</I>' AS XML) AS cEmailAddressXML
	    FROM #ImportLineRef
       WHERE cToRefMail <> ''
	)
	SELECT nImpLineId, 
		   LEFT(REPLACE( LOWER( RTRIM(LTRIM(EmailAddress.X.value('.', 'VARCHAR(256)')))), '&amp;', '&'),80) AS cContInf, 
		   '3' AS cContTyp,
		   ROW_NUMBER() OVER (PARTITION BY nImpLineId ORDER BY nImpLineId) as nSequence
      INTO #ImportLineMail		   
	  FROM myCTE
	      CROSS APPLY cEmailAddressXML.nodes('//I') EmailAddress(X);  

   -- Add any previously unseen emails addresses to RefCntact
   INSERT INTO RefContact ( cContTyp, cContInf)
   SELECT DISTINCT cContTyp, cContInf
     FROM #ImportLineMail ILM
    WHERE NOT EXISTS ( SELECT 1 FROM RefContact RC  WITH (NOLOCK) WHERE RC.cContTyp = '3' AND RC.cContInf = ILM.cContInf)
       
   -- Add a ImportRefContact Line for each email address in the file.
   INSERT INTO ImportRefContact ( nImpLineId, nContId, nSequence)
   SELECT ILM.nImpLineId, RC.nContId, ILM.nSequence
     FROM #ImportLineMail ILM WITH (NOLOCK) 
     JOIN RefContact RC WITH (NOLOCK) ON ILM.cContInf = RC.cContInf AND RC.cContTyp = 3

   -- clean up
   DROP TABLE #ImportLineMail;



   -- .INS. --
   EXEC usp_UpdateProgress 'Importing Instruction Data. INS', 50

    SELECT  cToRefCk   -- Payee
           ,cToCuId   -- Currency
           ,cToBkAc   -- BeneAcct
           ,cToTxt1   -- BeneText1
           ,cToTxt2   -- BeneText2
           ,cToSwift   -- ReceSwift
           ,cToBkTxt1   -- ReceText1
           ,cToBkTxt2   -- ReceText2
           ,cCvSwift1   -- CoverSwift1
           ,cCvTxt11   -- CoverText11
           ,cCvTxt12   -- CoverText12
           ,cToBkKey   -- BankKey
           ,cToBkCtId   -- BankCntry
           ,cToRgId   -- Region
           ,cToAcHldr   -- AccHolder
           ,cCvSwift2   -- CoverSwift2
           ,cCvTxt21   -- CoverText21
           ,cCvTxt22   -- CoverText22
           ,cIBAN   -- BeneAcctiban
           ,cToName   -- Name1 (original name for alternative payee)
           --,cImpRecId   -- 'S'   -- INS
		   ,'  ' AS cToCtId   -- Bene Country
		   ,CAST(null AS VARCHAR(120)) AS cToRefStreet -- Bene Street 
		   ,CAST(null AS VARCHAR(10)) AS cToRefPostal -- Bene Postal
		   ,CAST(null AS VARCHAR(80)) AS cToRefCity -- Bene City
		   ,cToBkBranch -- Bank Branch
		   --,cCheckSum -- we now add a computed column to the temp table to include fields from REF as well
		   ,nImpRecId
		   ,nStatus
		   ,cCmpCk
		   ,nImpLineId
      INTO #ImportLineIns
      FROM ImportLine 
     WHERE nImpFileId = @tnImpFileId AND 
           cImpRecTyp = 'S';

   CREATE INDEX IX_ImportLineIns_cToRefCk ON #ImportLineIns (cToRefCk)
   CREATE INDEX IX_ImportLineIns_nImpRecId_nStatus ON #ImportLineIns (nImpRecId, nStatus)

    -- add the bene country to the tabel
	UPDATE ILI
	   SET ILI.cToCtId = LEFT(ILR.cToRefAddr, 2),
	       ILI.cToRefStreet = ILR.cToRefStreet,
		   ILI.cToRefPostal = ILR.cToRefPostal,
		   ILI.cToRefCity = ILR.cToRefCity
	  FROM #ImportLineIns ILI WITH(NOLOCK)
	  JOIN #ImportLineRef ILR WITH(NOLOCK) ON ILR.cToRefCk = ILI.cToRefCk AND ILR.cCmpCK = ILI.cCmpCK

   ALTER TABLE #ImportLineIns ADD cCheckSum AS (CONVERT([varchar](32),hashbytes('MD5',(rtrim([cToBkAc])+';'+rtrim([cToTxt1])+';'+rtrim([cToTxt2])+';'+rtrim([cToSwift])+';'+rtrim([cToBkTxt1])+';'+rtrim([cToBkTxt2])+';'+rtrim([cCvSwift1])+';'+rtrim([cCvTxt11])+';'+rtrim([cCvTxt12])+';'+rtrim([cToBkKey])+';'+rtrim([cToBkCtId])+';'+rtrim([cToRgId])+';'+rtrim([cToAcHldr])+';'+rtrim([cCvSwift2])+';'+rtrim([cCvTxt21])+';'+rtrim([cCvTxt22])+';'+rtrim([cIBAN]))+';'+rtrim(isnull([cToName],''))+';'+rtrim(isnull([cToBkBranch],''))+';'+RTRIM(ISNULL(cToCtId,''))+';'+RTRIM(ISNULL(cToRefStreet,''))+';'+RTRIM(ISNULL(cToRefPostal,''))+';'+RTRIM(ISNULL(cToRefCity,''))),(2)))
   CREATE INDEX IX_ImportLineIns_cCheckSum ON #ImportLineIns (cCheckSum)

   -- If the Ins record already exists, update Import line with the Ins Id
   -- we have to manually add the to country, to street, to postal and to city to the comparison as the check sum on ImportLine can't contain it (value from a different row)


     	----Update Instruction that already exists with status 5 and previous Instruction Id
   UPDATE IL
      SET nStatus = 5,
          nImpRecId = nInsId
     FROM #ImportLineIns IL WITH (NOLOCK) 
     INNER JOIN INS WITH (NOLOCK) ON INS.cInsTypeId = 'S' AND INS.cCheckSum = IL.cCheckSum 
    WHERE ISNULL( IL.nImpRecId, 0) = 0 AND IL.nStatus = 0;

	
--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------
------------------------     Ins inserted one by one to catch errors            ------------------------------------
------------------------                CR                              --------------------------------------------
--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------

DECLARE		@cToBkAc varchar (50) ,
			@cToTxt1 varchar (160) ,
			@cToTxt2 varchar (80) ,
			@cToSwift varchar (50) ,
			@cToBkTxt1 varchar (140) ,
			@cToBkTxt2 varchar (50) ,
			@cCvSwift1 varchar (50) ,
			@cCvTxt11 varchar (60) ,
			@cCvTxt12 varchar (50) ,
			@cToBkKey varchar (50) ,
			@cToBkCtId char (2) ,
			@cToRgId char (3) ,
			@cToAcHldr varchar (120) ,
			@cCvSwift2 varchar (50) ,
			@cCvTxt21 varchar (60) ,
			@cCvTxt22 varchar (50) ,
			@cIBAN char (34)  ,
			@cToName varchar (140) ,
			@cToCtId char (2) ,
			@cToRefStreet varchar (120) ,
			@cToRefPostal varchar (10) ,
			@cToRefCity varchar (80) ,
			@cToBkBranch varchar (40) ,
			@cCheckSum varchar(32)

DECLARE InsToAdd CURSOR FAST_FORWARD FOR
-- Import the new instructions.
			SELECT DISTINCT		[cToBkAc]
								,[cToTxt1]
								,[cToTxt2]
								,[cToSwift]
								,[cToBkTxt1]
								,[cToBkTxt2]
								,[cCvSwift1]
								,[cCvTxt11]
								,[cCvTxt12]
								,[cToBkKey]
								,[cToBkCtId]
								,[cToRgId]
								,[cToAcHldr]
								,[cCvSwift2]
								,[cCvTxt21]
								,[cCvTxt22]
								,[cIBAN]
								,[cToName]
								,[cToCtId]
								,[cToRefStreet]
								,[cToRefPostal]
								,[cToRefCity]
								,[cToBkBranch]
			FROM #ImportLineIns WITH (NOLOCK) 
			WHERE ISNULL( nImpRecId, 0) = 0 AND nStatus = 0
OPEN InsToAdd
FETCH NEXT FROM InsToAdd
INTO	@cToBkAc ,
		@cToTxt1  ,
		@cToTxt2 ,
		@cToSwift ,
		@cToBkTxt1  ,
		@cToBkTxt2 ,
		@cCvSwift1  ,
		@cCvTxt11  ,
		@cCvTxt12  ,
		@cToBkKey  ,
		@cToBkCtId  ,
		@cToRgId ,
		@cToAcHldr  ,
		@cCvSwift2 ,
		@cCvTxt21 ,
		@cCvTxt22 ,
		@cIBAN ,
		@cToName,
		@cToCtId  ,
		@cToRefStreet  ,
		@cToRefPostal ,
		@cToRefCity ,
		@cToBkBranch 

WHILE @@FETCH_STATUS = 0 
BEGIN
	BEGIN TRY  
	
			SET  @cCheckSum = CONVERT(varchar(32),hashbytes('MD5',(((((((((((((((((((((((((((((((((((((((((((rtrim(@cToBkAc)+';')+rtrim(@cToTxt1))+';')+rtrim(@cToTxt2))+';')+rtrim(@cToSwift))+';')+rtrim(@cToBkTxt1))+';')+rtrim(@cToBkTxt2))+';')+rtrim(@cCvSwift1))+';')+rtrim(@cCvTxt11))+';')+rtrim(@cCvTxt12))+';')+rtrim(@cToBkKey))+';')+rtrim(@cToBkCtId))+';')+rtrim(@cToRgId))+';')+rtrim(@cToAcHldr))+';')+rtrim(@cCvSwift2))+';')+rtrim(@cCvTxt21))+';')+rtrim(@cCvTxt22))+';')+rtrim(@cIBAN))+';')+rtrim(isnull(@cToName,'')))+';')+rtrim(isnull(@cToBkBranch,'')))+';')+rtrim(isnull(@cToCtId,'')))+';')+rtrim(isnull(@cToRefStreet,'')))+';')+rtrim(isnull(@cToRefPostal,'')))+';')+rtrim(isnull(@cToRefCity,''))),(2));
		
			IF EXISTS
					(
						SELECT cCheckSum
						FROM dbo.Ins i
						WHERE i.cCheckSum = @cCheckSum
					)
					BEGIN
						----Update Instruction that already exists with status 5 and previous Instruction Id
						PRINT 'UPDATE =>' + @cCheckSum
						UPDATE IL
						SET nStatus = 5,
						nImpRecId = nInsId
						FROM #ImportLineIns IL WITH (NOLOCK) 
						INNER JOIN INS WITH (NOLOCK) ON INS.cInsTypeId = 'S' AND INS.cCheckSum = IL.cCheckSum 
						WHERE ISNULL( IL.nImpRecId, 0) = 0 AND IL.nStatus = 0 and INS.cCheckSum  = @cCheckSum;
					END
			ELSE
					BEGIN
						PRINT 'ADD =>' + @cCheckSum
						INSERT INTO [dbo].[Ins]
								(
								[cToBkAc]
								,[cToTxt1]
								,[cToTxt2]
								,[cToSwift]
								,[cToBkTxt1]
								,[cToBkTxt2]
								,[cCvSwift1]
								,[cCvTxt11]
								,[cCvTxt12]
								,[cToBkKey]
								,[cToBkCtId]
								,[cToRgId]
								,[cToAcHldr]
								,[cCvSwift2]
								,[cCvTxt21]
								,[cCvTxt22]
								,[lApproved]
								,[cIBAN]
								,[cToName]
								,[cToCtId]
								,[cToRefStreet]
								,[cToRefPostal]
								,[cToRefCity]
								,[cToBkBranch])
						VALUES	(
								@cToBkAc ,
								@cToTxt1  ,
								@cToTxt2 ,
								@cToSwift ,
								@cToBkTxt1  ,
								@cToBkTxt2 ,
								@cCvSwift1  ,
								@cCvTxt11  ,
								@cCvTxt12  ,
								@cToBkKey  ,
								@cToBkCtId  ,
								@cToRgId ,
								@cToAcHldr  ,
								@cCvSwift2 ,
								@cCvTxt21 ,
								@cCvTxt22 ,
								1,
								@cIBAN ,
								@cToName,
								@cToCtId  ,
								@cToRefStreet  ,
								@cToRefPostal ,
								@cToRefCity ,
								@cToBkBranch )
				END
		END TRY  
		BEGIN CATCH  
			INSERT INTO ImportLineNotAdded	(
											[nPerId] ,
											[nImpLineId] ,
											[nImpFileId] ,
											[cErrorMessage]
											)
			VALUES							(
											0,
											0,
											0,
											'Code Ins Tabble: '  + CAST(ERROR_NUMBER() AS VARCHAR) + ' : ' +CAST(ERROR_MESSAGE() AS VARCHAR(254))
											)
			
		END CATCH;  
	FETCH NEXT FROM InsToAdd
		INTO		@cToBkAc ,
					@cToTxt1  ,
					@cToTxt2 ,
					@cToSwift ,
					@cToBkTxt1  ,
					@cToBkTxt2 ,
					@cCvSwift1  ,
					@cCvTxt11  ,
					@cCvTxt12  ,
					@cToBkKey  ,
					@cToBkCtId  ,
					@cToRgId ,
					@cToAcHldr  ,
					@cCvSwift2 ,
					@cCvTxt21 ,
					@cCvTxt22 ,
					@cIBAN ,
					@cToName,
					@cToCtId  ,
					@cToRefStreet  ,
					@cToRefPostal ,
					@cToRefCity ,
					@cToBkBranch
	END

CLOSE InsToAdd
DEALLOCATE InsToAdd


   -- Run the Update again for the new instructions. this time nStatus = 6
   UPDATE IL
      SET nStatus = 6,
          nImpRecId = nInsId
     FROM #ImportLineIns IL WITH (NOLOCK) 
     INNER JOIN INS WITH (NOLOCK) ON INS.cInsTypeId = 'S' AND INS.cCheckSum = IL.cCheckSum 
    WHERE ISNULL( IL.nImpRecId, 0) = 0  AND IL.nStatus = 0;
       
   EXEC usp_UpdateProgress 'Importing Invoice Data. INV', 65

    SELECT nImpLineId
		  ,nImpFileId
	      ,nImpRecId
  		  ,nAmt 
		  ,nCashDisc 
		  ,nStatus
		  ,cApproved
		  ,cBlocked
		  ,cCmpCk
		  ,cCentralBankIndicator
		  ,cItmTxt
		  ,cIndivPmt
		  ,cImpRecTyp
		  ,cPaymentMethod
		  ,cPaymentReference
          ,cPmtRefCmpCK
		  ,cRefInvId 
		  ,cSapDocId
		  ,cToCUId
		  ,cToRefCk
		  ,cToSwift
		  ,cToBkAc 
		  ,cToBkKey
		  ,dIssue 
		  ,dDue 
		  ,dPayment
		  ,cPaymentOrderId
      INTO #ImportLine
      FROM ImportLine 
     WHERE nImpFileId = @tnImpFileId AND 
           cImpRecTyp = 'I';

   CREATE INDEX IX_ImportLineRef_cSapDocId ON #ImportLine (cSapDocId)

    BEGIN TRANSACTION;
   
   -- INV
   -- Indentify invoices that are alrady in the system. Those that can be reimport flag with 2, otherwsie flag with -1
   --IF EXISTS(  SELECT CASE WHEN OBJECT_ID('tempdb..#Inv_Audit_Update') IS  NULL THEN 1 ELSE 0 END)
   --   CREATE TABLE #Inv_Audit_Update (Dummy bit)

   -- import SAP OR SFP files
   IF EXISTS( SELECT cImpTyp FROM ImportFile WITH (NOLOCK) WHERE nImpFileId = @tnImpFileId AND cImpTyp IN ('S','F'))
   BEGIN
	   UPDATE IL
		  SET IL.nStatus = CASE WHEN I.lBlocked = 1 AND ISNULL(I.nPmtId, 0) = 0 THEN 2 ELSE -2 END,
			  IL.nImpRecId = CASE WHEN I.lBlocked = 1 AND ISNULL(I.nPmtId, 0) = 0 THEN I.nInvId ELSE 0 END
		  FROM #ImportLine IL
		  JOIN Inv I WITH (NOLOCK) ON IL.cSapDocId = I.cSapDocId
		 WHERE IL.cImpRecTyp = 'I'

	   -- 20130719: RM:
	   -- cInvTypId depends on import type (SAP or SFP)
	   -- lIndivPmt will be set to 1 for all SFP invoices

	   -- Update the exsiting invoices that we can.
	   UPDATE I
		  SET nPerId                = ImF.nPerId,
			  nToInsId              = ILS.nImpRecId, 
			  nRefAddrId            = ILR.nImpRecId, 
			  cToCuId               = ISNULL( CM.cCUId, IL.cToCUId), 
			  nAmt                  = IL.nAmt, 
			  cRefInvId             = IL.cRefInvId, 
			  dIssue                = IL.dIssue, 
			  dDue                  = IL.dDue, 
			  dPmt                  = IL.dPayment, 
			  lApproved             = CASE WHEN IL.cApproved='APPR' THEN 1 ELSE 0 END, 
			  lBlocked              = CASE WHEN IL.cBlocked='UNBL' THEN 0 ELSE 1 END, 
			  nCashDisc             = IL.nCashDisc, 
			  cItmTxt               = LEFT( IL.cItmTxt, 50), 
			  cInvStatId            = 'I',
			  cInvTypId             = CASE WHEN ImF.cImpTyp = 'S' THEN 'N' ELSE ImF.cImpTyp END,
			  lIndivPmt             = CASE WHEN IL.cIndivPmt = 'X' OR ImF.cImpTyp = 'F' THEN 1 ELSE 0 END,
			  nImpLineId            = IL.nImpLineId,
			  cCmpCk                = IL.cCmpCk,
			  cCentralBankIndicator = IL.cCentralBankIndicator,
			  cPaymentMethod        = IL.cPaymentMethod,
			  cPaymentReference     = LEFT( IL.cPaymentReference, 50), 			                                 
              cPmtRefCmpCK          = IL.cPmtRefCmpCK,
			  cPaymentOrderId       = IL.cPaymentOrderId
		   FROM Inv I WITH (NOLOCK)
		   JOIN #ImportLine IL ON IL.nImpRecId = I.nInvId AND IL.nStatus = 2 AND IL.cImpRecTyp = 'I'
	  LEFT JOIN CurrencyMaster CM WITH (NOLOCK) ON IL.cToCUId = CM.cSapCuId
		   JOIN ImportFile ImF ON IL.nImpFileId= ImF.nImpFileId 
		   JOIN #ImportLineRef ILR ON ILR.nImpRecId > 0 AND IL.cCmpCk = ILR.cCmpCk AND IL.cToRefCk = ILR.cToRefCk
		   JOIN #ImportLineIns ILS ON IL.cCmpCk = ILS.cCmpCk AND ILS.nImpRecId > 0 AND  
								  IL.cToRefCk = ILS.cToRefCk AND 
								  IL.cToCuId  = ILS.cToCuId  AND
								  IL.cToSwift = ILS.cToSwift AND
								  IL.cToBkAc  = ILS.cToBkAc  AND 
								  IL.cToBkKey = ILS.cToBkKey

--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------
------------------------     Invs checks before final insert            --------------------------------------------
------------------------                CR                              --------------------------------------------
--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------
DECLARE	@nPerId [int],
		@nToInsId [int],
		@nRefAddrId [int] ,
		@cToCuId [char](3),
		@nAmt [numeric](20, 2),
		@cRefInvId [varchar](50),
		@dIssue [datetime],
		@dDue [datetime],
		@dPmt [datetime],
		@lApproved [bit],
		@lBlocked [bit],
		@cSapDocId [char](25),
		@nCashDisc [numeric](9, 2),
		@cItmTxt [varchar](50),
		@cInvStatId [char](1),
		@cInvTypId [char](1),
		@lIndivPmt [bit],
		@nImpLineId [int],
		@cCmpCk [char](16),
		@cCentralBankIndicator [varchar](10),
		@cPaymentMethod [varchar](1),
		@cPaymentReference [varchar](50),
		@cPmtRefCmpCK [varchar](50) ,
		@cPaymentOrderId [varchar](20)

DECLARE InvsToAdd CURSOR FAST_FORWARD FOR
-- Import the new invoices.
				  SELECT DISTINCT
				         ImF.nPerId,
						 ILS.nImpRecId,
						 ILR.nImpRecId, 
						 ISNULL( CM.cCUId, IL.cToCUId), 
						 IL.nAmt, 
						 IL.cRefInvId + CASE
											WHEN ImF.cImpTyp = 'F' AND LEN(IL.cSapDocId) >= 24 AND SUBSTRING(IL.cSapDocId, 19, 3) <> '001'
												THEN
													CASE
														WHEN IL.cCmpCk = 'NCN66' 
															THEN SUBSTRING(IL.cSapDocId, 20, 2)
														ELSE '-' + SUBSTRING(IL.cSapDocId, 19, 3) END
												ELSE '' 
										END, 
						 IL.dIssue, 
						 IL.dDue, 
						 IL.dPayment, 
						 CASE WHEN IL.cApproved='APPR' THEN 1 ELSE 0 END, 
						 CASE WHEN IL.cBlocked='UNBL' THEN 0 ELSE 1 END, 
						 IL.cSAPDocId,
						 IL.nCashDisc, 
						 LEFT( IL.cItmTxt, 50), 
						 'I',
						 CASE WHEN ImF.cImpTyp = 'S' THEN 'N' ELSE ImF.cImpTyp END,
						 CASE WHEN IL.cIndivPmt = 'X' OR ImF.cImpTyp = 'F' THEN 1 ELSE 0 END,
						 IL.nImpLineId,
						 IL.cCmpCk,
						 LEFT( IL.cCentralBankIndicator, 50),
			             IL.cPaymentMethod,
			             --CASE WHEN ISNULL((SELECT 1 FROM Inbox I WITH (NOLOCK) WHERE ImF.nInboxId = I.nInboxId AND I.cFileName LIKE '%SCFX%'),0) = 1 THEN 'Spanish Platform' ELSE LEFT( IL.cPaymentReference, 50) END,
						 LEFT( IL.cPaymentReference, 50),
                         IL.cPmtRefCmpCK,
						 IL.cPaymentOrderId
					FROM #ImportLine IL 
			   LEFT JOIN CurrencyMaster CM WITH (NOLOCK) ON IL.cToCUId = CM.cSapCuId
					JOIN ImportFile ImF WITH (NOLOCK) ON IL.nImpFileId= ImF.nImpFileId 
					--JOIN #ImportLineRef ILR WITH (NOLOCK) ON ILR.nImpRecId > 0 AND IL.cCmpCk = ILR.cCmpCk AND IL.cToRefCk = ILR.cToRefCk
					-- prevent duplicate errors. Take MAX(nImpRecId/nRefAddrId)
					--/*
					JOIN (SELECT MAX(nImpRecId) AS nImpRecId, cCmpCk, cToRefCk 
					        FROM #ImportLineRef 
						   WHERE nImpRecId>0 
						   GROUP BY cCmpCk, cToRefCk) ILR ON IL.cCmpCk = ILR.cCmpCk AND IL.cToRefCk = ILR.cToRefCk
					--*/
					JOIN #ImportLineIns ILS WITH (NOLOCK) ON ILS.nImpRecId = ( SELECT TOP 1 ILS2.nImpRecId 
					                                                             FROM #ImportLineIns ILS2 WITH (NOLOCK)
					                                                            WHERE ILS2.nImpRecId > 0 AND 
										                                                IL.cCmpCk = ILS2.cCmpCk AND 
										                                                IL.cToRefCk = ILS2.cToRefCk AND 
										                                                IL.cToCuId  = ILS2.cToCuId  AND
										                                                IL.cToSwift = ILS2.cToSwift AND
										                                                IL.cToBkAc  = ILS2.cToBkAc  AND 
										                                                IL.cToBkKey = ILS2.cToBkKey)
				   WHERE IL.nStatus = 0 AND IL.cImpRecTyp = 'I' AND LEN( IL.cCmpCK) < 17


OPEN InvsToAdd
FETCH NEXT FROM InvsToAdd
INTO 	@nPerId,
		@nToInsId,
		@nRefAddrId ,
		@cToCuId,
		@nAmt ,
		@cRefInvId,
		@dIssue,
		@dDue,
		@dPmt,
		@lApproved ,
		@lBlocked ,
		@cSapDocId ,
		@nCashDisc ,
		@cItmTxt ,
		@cInvStatId ,
		@cInvTypId ,
		@lIndivPmt ,
		@nImpLineId,
		@cCmpCk ,
		@cCentralBankIndicator ,
		@cPaymentMethod ,
		@cPaymentReference ,
		@cPmtRefCmpCK  ,
		@cPaymentOrderId

WHILE @@FETCH_STATUS = 0 
BEGIN
	BEGIN TRY  
		 INSERT INTO Inv ( 
										 nPerId, 
										 nToInsId, 
										 nRefAddrId, 
										 cToCuId, 
										 nAmt, 
										 cRefInvId, 
										 dIssue, 
										 dDue, 
										 dPmt, 
										 lApproved, 
										 lBlocked, 
										 cSapDocId, 
										 nCashDisc, 
										 cItmTxt, 
										 cInvStatId, 
										 cInvTypId,
										 lIndivPmt, 
										 nImpLineId,
										 cCmpCk,
										 cCentralBankIndicator,
										 cPaymentMethod,
										 cPaymentReference,
										 cPmtRefCmpCK,
										 cPaymentOrderId
									)
				VALUES	(@nPerId,
						@nToInsId,
						@nRefAddrId ,
						@cToCuId,
						@nAmt ,
						@cRefInvId,
						@dIssue,
						@dDue,
						@dPmt,
						@lApproved ,
						@lBlocked ,
						@cSapDocId ,
						@nCashDisc ,
						@cItmTxt ,
						@cInvStatId ,
						@cInvTypId ,
						@lIndivPmt ,
						@nImpLineId,
						@cCmpCk ,
						@cCentralBankIndicator ,
						@cPaymentMethod ,
						@cPaymentReference ,
						@cPmtRefCmpCK  ,
						@cPaymentOrderId)
		END TRY  
		BEGIN CATCH  
			INSERT INTO ImportLineNotAdded	(
											[nPerId] ,
											[nImpLineId] ,
											[nImpFileId] ,
											[cErrorMessage]
											)
			VALUES							(
											ISNULL(@nPerId,0),
											@nImpLineId,
											@tnImpFileId,
											'Code : '  + CAST(ERROR_NUMBER() AS VARCHAR) + ' : ' +CAST(ERROR_MESSAGE() AS VARCHAR(254))
											)
			
		END CATCH;  
	FETCH NEXT FROM InvsToAdd
		INTO	@nPerId,
				@nToInsId,
				@nRefAddrId ,
				@cToCuId,
				@nAmt ,
				@cRefInvId,
				@dIssue,
				@dDue,
				@dPmt,
				@lApproved ,
				@lBlocked ,
				@cSapDocId ,
				@nCashDisc ,
				@cItmTxt ,
				@cInvStatId ,
				@cInvTypId ,
				@lIndivPmt ,
				@nImpLineId,
				@cCmpCk ,
				@cCentralBankIndicator ,
				@cPaymentMethod ,
				@cPaymentReference ,
				@cPmtRefCmpCK  ,
				@cPaymentOrderId
	END

CLOSE InvsToAdd
DEALLOCATE InvsToAdd

	   -- Put the backlink    
	   UPDATE IL
		  SET IL.nStatus = 7,
			  IL.nImpRecId = I.nInvId
		  FROM #ImportLine IL
			   JOIN Inv I WITH (NOLOCK) ON IL.cSapDocId = I.cSapDocId
		 WHERE IL.cImpRecTyp = 'I' AND IL.nStatus = 0
   END
   ELSE
   BEGIN
	   DECLARE @lnDummyIns INT
	   SELECT @lnDummyIns = MAX(nInsId) FROM ins WITH (NOLOCK) WHERE cRefCk = 'DUMMY'
	   
	   -- Import the new (M)anual Invoices.
	   INSERT INTO Inv ( nPerId, 
						 nToInsId,
						 nRefAddrId, 
						 cToCuId, 
						 nAmt, 
						 cRefInvId, 
						 lApproved, 
						 lBlocked, 
						 cSapDocId, 
						 nCashDisc, 
						 cItmTxt, 
						 cInvStatId, 
						 cInvTypId,
						 lIndivPmt,
						 nImpLineId, 
						 dIssue,
						 dDue,
						 dPmt,
						 cCmpCk,
						 cCentralBankIndicator,
						 cPaymentMethod,
						 cPaymentReference,
						 cPmtRefCmpCK,
						 cPaymentOrderId)
				  SELECT ImF.nPerId,
						 CASE WHEN C.cCmpCk IS NULL THEN S.nInsId ELSE @lnDummyIns END, 
						 RA.nRefAddrId, 
						 ISNULL( CM.cCUId, IL.cToCUId), 
						 IL.nAmt, 
						 IL.cRefInvId, 
						 1, 
						 0, 
						 '_A' + LTRIM( STR( IL.nImpLineId, 14, 0)),
						 0, 
						 '', 
						 'I',
						 'M',
						 0,
						 IL.nImpLineId,
						 '19000101',
						 '19000101',
						 '19000101',
						 IL.cCmpCk,
						 IL.cCentralBankIndicator,
			             IL.cPaymentMethod,
			             LEFT( IL.cPaymentReference, 50),
                         IL.cPmtRefCmpCK,
						 IL.cPaymentOrderId
					FROM #ImportLine IL 
			   LEFT JOIN CurrencyMaster CM WITH (NOLOCK) ON IL.cToCUId = CM.cSapCuId
					JOIN ImportFile ImF WITH (NOLOCK) ON IL.nImpFileId= ImF.nImpFileId 
					JOIN RefMain RM WITH (NOLOCK) ON IL.cToRefCK = RM.cRefCk AND RM.cSystemId = dbo.GetRefSystemId(IL.cToRefCk, @cSystemId)
					JOIN RefAddress RA WITH (NOLOCK) ON RA.nRefId = RM.nRefId AND RA.nRefAddrId = ( SELECT MAX( RA2.nRefAddrId) FROM RefAddress RA2 WHERE RA2.nRefId = RM.nRefId)
			   LEFT JOIN Company C WITH (NOLOCK) ON IL.cToRefCK = C.cCmpCK               
			   LEFT JOIN Ins S WITH (NOLOCK) ON IL.cToRefCK = S.cRefCk AND tCreated = ( SELECT MAX(  tCreated) FROM Ins I2 WITH (NOLOCK) WHERE I2.cRefCk = S.cRefCK)
				   WHERE IL.nStatus = 0 AND IL.cImpRecTyp = 'I'

	   -- Put the backlink    
	   UPDATE IL
		  SET IL.nStatus = 8,
			  IL.nImpRecId = I.nInvId
		  FROM #ImportLine IL
			   JOIN Inv I WITH (NOLOCK) ON '_A' + LTRIM( STR( IL.nImpLineId, 14, 0)) = I.cSapDocId
		 WHERE IL.cImpRecTyp = 'I'
   END

-- MKS. Leave these invoices as unprocessed.. we might want to try and reimport them later?
--   UPDATE IL
--      SET IL.nStatus = -3
--      FROM #ImportLine IL
--     WHERE IL.cImpRecTyp = 'I' AND IL.nStatus = 0 -- No matchin invoice. Must be a missing Ins or Ref!
      
       
   -- Final step use the working table to update the real table
   UPDATE ImportLine
      SET nStatus = W.nStatus,
          nImpRecId = W.nImpRecId
     FROM ImportLine IL WITH (NOLOCK)
     JOIN #ImportLine W WITH (NOLOCK) ON IL.nImpLineId = W.nImpLineId
    WHERE IL.nImpFileId = @tnImpFileId;


   --IF EXISTS(  SELECT CASE WHEN OBJECT_ID('tempdb..#Inv_Audit_Update') IS  NULL THEN 0 ELSE 1 END)
   --   DROP TABLE #Inv_Audit_Update
   
   COMMIT TRANSACTION;

   DROP TABLE #ImportLine

   EXEC usp_UpdateProgress 'Importing Withholding Tax Data. WHT', 80

   -- 4 WHT
   SELECT cWhtId1 AS cWHTId, 
          cSapDocId AS cSAPDocId,
          nWHTAmt1 AS nAmt
     INTO #ImportLineWHT
     FROM ImportLine 
    WHERE nImpFileId = @tnImpFileId AND 
          cImpRecTyp = 'W' AND 
          cWhtId1 <> ''
    UNION ALL
   SELECT cWhtId2,
          cSapDocId AS cSAPDocId,
          nWHTAmt2 AS nAmt
     FROM ImportLine 
    WHERE nImpFileId = @tnImpFileId AND 
          cImpRecTyp = 'W' AND 
          cWhtId2 <> ''
    UNION ALL
   SELECT cWhtId3,
          cSapDocId AS cSAPDocId,
          nWHTAmt3 AS nAmt
     FROM ImportLine 
    WHERE nImpFileId = @tnImpFileId AND 
          cImpRecTyp = 'W' AND 
          cWhtId3 <> ''   
   UNION ALL    
   SELECT cWhtId4,
          cSapDocId AS cSAPDocId,
          nWHTAmt4 AS nAmt
     FROM ImportLine 
    WHERE nImpFileId = @tnImpFileId AND 
          cImpRecTyp = 'W' AND 
          cWhtId4 <> ''
   UNION ALL          
   SELECT cWhtId5,
          cSapDocId AS cSAPDocId,
          nWHTAmt5 AS nAmt
     FROM ImportLine 
    WHERE nImpFileId = @tnImpFileId AND 
          cImpRecTyp = 'W' AND 
          cWhtId5 <> ''   

   -- delete any WHT Records that already exist for the inv.
   DELETE WI
     FROM WhtInv WI WITH (NOLOCK)
     JOIN Inv I WITH (NOLOCK) ON WI.nInvId = I.nInvId
     JOIN #ImportLineWHT TMP WITH (NOLOCK) ON I.cSapDocId=TMP.cSAPDocId
     
   INSERT
     INTO WhtInv ( nWhtId, nInvId, nWHTAmt)
   SELECT WD.nWhtId,
          I.nInvId,
          TMP.nAmt
     FROM #ImportLineWHT TMP WITH (NOLOCK)
     JOIN Inv I WITH (NOLOCK) ON I.cSapDocId=TMP.cSAPDocId
     JOIN WhtDefinition WD WITH (NOLOCK) ON WD.cWhtId = TMP.cWHTId

   DROP TABLE #ImportLineWHT

   EXEC usp_UpdateProgress 'Updating Import Lines', 90

   UPDATE ImportLine
      SET nStatus = W.nStatus,
          nImpRecId = W.nImpRecId
     FROM ImportLine IL WITH (NOLOCK)
     JOIN #ImportLineRef W WITH (NOLOCK) ON IL.nImpLineId = W.nImpLineId
    WHERE IL.nImpFileId = @tnImpFileId; 

   UPDATE ImportLine
      SET nStatus = W.nStatus,
          nImpRecId = W.nImpRecId
     FROM ImportLine IL WITH (NOLOCK)
     JOIN #ImportLineIns W WITH (NOLOCK) ON IL.nImpLineId = W.nImpLineId
    WHERE IL.nImpFileId = @tnImpFileId; 


   DROP TABLE #ImportLineRef
   DROP TABLE #ImportLineIns

END
-- Update ImportFile set nPerId = 3 where nImpFileId = 9993
-- select * from Inv where tLastUpdt > '20120907'
-- 11.06.2012 11:08:42 - NOE\MASKEELS Comment: make sure we only insert distinct instructions.
-- 11.06.2012 11:28:27 - NOE\MASKEELS Comment: Use a #temp table. the computed columns distinct or group was really too slow.
-- 11.06.2012 15:24:23 - NOE\MASKEELS Comment: removed the hard-coding.
-- 31.08.2012 16:44:01 - NOE\MASKEELS Comment: started to put the RefContact info into the file.
-- 04.09.2012 16:44:01 - NOE\MASKEELS Comment: all ref contact info now imported with the sp except mails.
-- 17.09.2012 17:44:01 - NOE\MASKEELS Comment: all inv info now imported with the sp.
-- 27.09.2012 11:33:01 - NOE\MASKEELS Comment: Refaddress line missed the scoping! 
-- 01.10.2012 19:02:44 - NOE\MASKEELS Comment: Used new column cCheckSum on the Ins. Makes sure we only use Ref and Ins lines that have actually been imported.
-- 02.10.2012 19:02:44 - NOE\MASKEELS Comment: Created unique statuses for each Line Type and event.
-- 03.10.2012 19:02:44 - NOE\MASKEELS Comment: Import the References outisde of the transaction. Use a seperate temp table to import them.
-- 04.10.2012 19:02:44 - NOE\MASKEELS Comment: Added progress messages. Moved the INS outside of the trasnaction.
-- 15.10.2012 16:00:00 - NOE\TIBATIST Comment: Added cInsTypeId = 'S' condition when joining with the Ins table
-- 18.10.2012 16:00:00 - NOE\MASKEELS Comment: Added encoding/decoding for '&' in the email address.
-- 29.10.2012 12:12:00 - NOE\TIBATIST Comment: FIXED encoding/decoding for '&' in the email address.
-- 05.11.2012 12:12:00 - NOE\ROMEYER Comment: FIXED fax linking.
-- 05.11.2012 12:12:00 - NOE\MASKEELS Comment: Only import unique fax numbers per import line. Ignore fax 2.
-- 09.11.2012 12:48:00 - NOE\TIBATIST Comment: Populated cCmpCk on Inv table.
-- 09.11.2012 17:33:00 - NOE\TIBATIST Comment: Added DISTINCT when inserting Invoices
-- 29.01.2013 20:12:00 - NOE\TIBATIST Comment: Added cCheckSum computed column to Ins. No need to update IBAN on Ins.
-- 13.02.2013          - NOE\TIBATIST Comment: Remove the nFmRefId. All Inv transactions are now assumed to be from a company.
-- 18.04.2013 14:00:00 - NOE\TIBATIST Comment: Added LEFT(REPLACE( LOWER( RTRIM(LTRIM(EmailAddress.X.value('.', 'VARCHAR(256)')))), '&amp;', '&'),80) to truncate email addresses in RefContact
-- 08.07.2013 10:42:00 - NOE\ROMEYER Comment: Added LEFT(cToAcHldr, 60) to Ins as we received a longer record
-- 19.07.2013 12:42:00 - NOE\ROMEYER Comment: Added SFP logic
-- 17.10.2013 15:34:00 - NOE\MASKEELS Comment: Added a filter to only import References where the code is less than 17 chars so that we don't cotinually have a problem with double byte vendor references.
-- 25.11.2013 13:36:55 - NOE\TIBATIST Comment: Removed LEFT(cToAcHldr, 60). Expanded column cToAcHldr to 120.
-- 02.12.2013 18:14:00 - NOE\TIBATIST Comment: Added cToName to Ins
-- 03.12.2013 10:50:00 - NOE\TIBATIST Comment: Added cCentralBankIndicator, cPaymentMethod, cPaymentReference
-- 24.01.2014 11:14:00 - NOE\ROMEYER Comment: Added Ref Type E (Employee) for SAP ByD
-- 25.03.2014 18:08:00 - NOE\TIBATIST Comment: Removed cItmTxt unused logic
-- 07.05.2014 11:26:00 - NOE\ROMEYER Comment: Added Ref Type E (Employee) for SAP ByD FI Employee
-- 05.03.2015 10:59:52 - NOE\ROMEYER Comment: Added new fields in download file (pan code and cToCtId)
-- 05.03.2015 15:00:11 - NOE\ROMEYER Comment: Fix for the pan code
-- 16.11.2015 15:43:12 - NOE\TIBATIST Comment: Added cToRefStreet, cToRefPostal, cToRefCity and cToBkBranch
-- 14.01.2016 00:06:11 - NOE\ROMEYER Comment: Fix for the pan code
-- 20.12.2016 12:51:15 - NOE\TIBATIST Comment: Added filter (ILR.cCmpCK = ILI.cCmpCK) when updating the bene street and city
-- 28.12.2016 14:06:59 - NOE\MASKEELS Comment: Rolled back the last change until the reason for the the wrong receiving bank continually being used is resolved. 
-- 28.12.2016 15:12:12 - NOE\TIBATIST Comment: E5678 - Added back the filter (ILR.cCmpCK = ILI.cCmpCK)
-- 07.06.2017 22:37:04 - NOE\ROMEYER Comment: PA: Only do SCF line item handling for P20 doc id's
-- 20.09.2017 22:55:03 - NOE\ROMEYER Comment: PA: NOCOUNT should be off so that we get a return value in .net (otherwise we get -1)
-- 07.05.2018 11:11:11 - NOE\ROMEYER Comment: E7715: Added system id and removed ImportRefConversion
-- 24.05.2018 19:09:31 - NOE\ROMEYER Comment: E7715: Special handling for group companies, we don't overwrite ref address with what we receive from the ERP
-- 28.06.2018 19:36:12 - NOE\TIBATIST Comment: Get TOP 1 ILS2.nImpRecId instead of MAX (ILS2.nImpRecId)
-- 14.02.2019 19:36:12 - NOE\MASKEELS Comment: Only update the RefMain system, if there is not already a record with the target system.
-- 04.01.2019 12:27:00 - NOE\MASKEELS Comment: Truncate Payment Reference to 50 
-- 01.04.2019 20:28:12 - NOE\TIBATIST Comment: Added CASE WHEN ISNULL((SELECT 1 FROM Inbox I WITH (NOLOCK) WHERE ImF.nInboxId = I.nInboxId AND I.cFileName LIKE '%SCFX%'),0) = 1 THEN 'Spanish Platform' ELSE LEFT( IL.cPaymentReference, 50) END,
-- 02.09.2020 13:49:31 - NOE\ROMEYER Comment: PA: Added cPaymentOrderId to import.
-- 10.09.2020 18:00:31 - NOE\ROMEYER Comment: PA: Don't disable trigger on unblocking invoices so that tLastUpdt gets updated.
-- 09.05.2021 12:24:31 - NOE\ROMEYER Comment: E18056: New checksum calculation.
-- 08.06.2022 16:53:00 - NOE\PYLILUOM Comment: E26806: Fixed duplicate invoice issue. Use a MAX() clause when joining to #ImportLineRef
-- 2022.08.16 14:41:00 - NSN-INTRA\LUTTMANN Comment: E27400: Do not add '-' for RefInvId when CmpCk = NCN66
-- 2022.08.16 14:41:00 - NSN-INTRA\BRUCE Comment: Q28785: when CmpCk = NCN66 Only 2 digit added ON REFID 
-- 2022.08.16 14:41:00 - NSN-INTRA\BRUCE Comment: E28927: Security on import file adding
-- 2022.08.16 14:41:00 - NSN-INTRA\BRUCE Comment: iNS ADDED SECURITY
GO

