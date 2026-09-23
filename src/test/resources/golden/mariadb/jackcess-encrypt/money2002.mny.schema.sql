-- AccessConverter - source: money2002.mny (MSISAM)
-- Target: MariaDB 10.11 or later, collation utf8mb4_uca1400_as_ci

SET NAMES utf8mb4;
SET @ac_sql_mode = @@SESSION.sql_mode, @ac_fk = @@SESSION.foreign_key_checks, @ac_ac = @@SESSION.autocommit;
SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_VALUE_ON_ZERO,NO_ENGINE_SUBSTITUTION';
SET SESSION foreign_key_checks = 0, autocommit = 0;

-- 1. Tables

CREATE TABLE `ACCT` (
  `hacct` INT NOT NULL,
  `ast` INT,
  `at` INT,
  `szAls` VARCHAR(8),
  `hacctRel` INT,
  `szFull` VARCHAR(48),
  `haddr` INT,
  `amtLimit` DECIMAL(19,4),
  `amtOpen` DECIMAL(19,4),
  `amtOpenRec` DECIMAL(19,4),
  `amtEndRec` DECIMAL(19,4),
  `dtOpen` DATETIME,
  `dtEndRec` DATETIME,
  `dtOpenRec` DATETIME,
  `fFavorite` BOOLEAN NOT NULL DEFAULT 0,
  `fClosed` BOOLEAN NOT NULL DEFAULT 0,
  `fInRec` BOOLEAN NOT NULL DEFAULT 0,
  `fTaxRel` BOOLEAN NOT NULL DEFAULT 0,
  `fRetirement` BOOLEAN NOT NULL DEFAULT 0,
  `mComment` LONGTEXT,
  `hfi` INT,
  `mContact` LONGTEXT,
  `mFax` LONGTEXT,
  `mVoice` LONGTEXT,
  `hcrnc` INT,
  `fVatEnabled` BOOLEAN NOT NULL DEFAULT 0,
  `grp` INT,
  `fStmtRequested` BOOLEAN NOT NULL DEFAULT 0,
  `mNum` LONGTEXT,
  `fAutobalance` BOOLEAN NOT NULL DEFAULT 0,
  `fDebtPlan` BOOLEAN NOT NULL DEFAULT 0,
  `cpm` INT,
  `dIntRate` DOUBLE,
  `dtIntRateChg` DATETIME,
  `dIntRateChg` DOUBLE,
  `frqDebtPay` INT,
  `cFrqInstDebtPay` DOUBLE,
  `dtNextDebtPay` DATETIME,
  `dPayRateMin` DOUBLE,
  `amtPayMin` DECIMAL(19,4),
  `amtSpending` DECIMAL(19,4),
  `hcatInterest` INT,
  `hcatPrincipal` INT,
  `hcatService` INT,
  `hcatOpenAdj` INT,
  `hcatEndAdj` INT,
  `fEmpMatch` BOOLEAN NOT NULL DEFAULT 0,
  `fEmpMatchPost` BOOLEAN NOT NULL DEFAULT 0,
  `hcatEmpMatch` INT,
  `hcatEmpMatchPost` INT,
  `fAdjustAmtPost` BOOLEAN NOT NULL DEFAULT 0,
  `amtBalloon` DECIMAL(19,4),
  `amtPI` DECIMAL(19,4),
  `amtPayment` DECIMAL(19,4),
  `frq` INT,
  `cFrqInst` DOUBLE,
  `frqCpd` INT,
  `cFrqInstCpd` DOUBLE,
  `iPmtMax` INT,
  `rateUser` DOUBLE,
  `rateCalc` DOUBLE,
  `rateAPR` DOUBLE,
  `fCanadian` BOOLEAN NOT NULL DEFAULT 0,
  `fCoupon` BOOLEAN NOT NULL DEFAULT 0,
  `fLent` BOOLEAN NOT NULL DEFAULT 0,
  `fARM` BOOLEAN NOT NULL DEFAULT 0,
  `dtNextAdj` DATETIME,
  `frqAdj` INT,
  `cFrqInstAdj` DOUBLE,
  `ctrnPartialChecks` INT,
  `fGotInterest` BOOLEAN NOT NULL DEFAULT 0,
  `fGotService` BOOLEAN NOT NULL DEFAULT 0,
  `szIdPrint` VARCHAR(13),
  `mRecIntraSyncToken` LONGTEXT,
  `lDebtHtrn` INT,
  `dtClose` DATETIME,
  `grfAdvice` INT,
  `dtCBPost` DATETIME,
  `lCB` INT,
  `olatHint` INT,
  `dtCBCut` DATETIME,
  `lHcrncOrig` INT,
  `fBusiness` BOOLEAN NOT NULL DEFAULT 0,
  `lHacctBonus` INT,
  `lHacctMonthly` INT,
  `fTaxExport` BOOLEAN NOT NULL DEFAULT 0,
  `uat` INT,
  `dtSerial` DATETIME,
  `fUpdated` BOOLEAN NOT NULL DEFAULT 0,
  `fWatch` BOOLEAN NOT NULL DEFAULT 0,
  `fAutoRecon` BOOLEAN NOT NULL DEFAULT 0,
  `fIueIncomplete` BOOLEAN NOT NULL DEFAULT 0,
  `amtOpenSaved` DECIMAL(19,4),
  `lHcrncOpenSaved` INT,
  `lHpgmCur` INT,
  `taat` INT,
  `dtPurchase` DATETIME,
  `amtPurchase` DECIMAL(19,4),
  `dUnitRec` DOUBLE,
  `mMCId` LONGTEXT,
  `dtExpire` DATETIME,
  `fAdvAbove` BOOLEAN NOT NULL DEFAULT 0,
  `amtAdvAbove` DECIMAL(19,4),
  `fAdvBelow` BOOLEAN NOT NULL DEFAULT 0,
  `amtAdvBelow` DECIMAL(19,4),
  `Balance Sort` INT,
  PRIMARY KEY (`hacct`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `ADDR` (
  `haddr` INT NOT NULL,
  `addrt` INT,
  `mNameUserLast` LONGTEXT,
  `mNameUserFirst` LONGTEXT,
  `mNameUserMiddle` LONGTEXT,
  `mId` LONGTEXT,
  `mAddrEmail` LONGTEXT,
  `mAddr1` LONGTEXT,
  `mAddr2` LONGTEXT,
  `mAddr3` LONGTEXT,
  `mCity` LONGTEXT,
  `mState` LONGTEXT,
  `mCountry` LONGTEXT,
  `lISOCountry` INT,
  `mPostalCode` LONGTEXT,
  `mPhoneHome` LONGTEXT,
  `mPhoneWork` LONGTEXT,
  `mFax` LONGTEXT,
  `mURL` LONGTEXT,
  `mFreeForm` LONGTEXT,
  PRIMARY KEY (`haddr`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `ADV` (
  `hadv` INT NOT NULL,
  `lAdvId` INT,
  `lType` INT,
  `lStatus` INT,
  `dtTrigger` DATETIME,
  `dtSerial` DATETIME,
  `lAdvCat` INT,
  `lAdvGoal` INT,
  `lBaseImportance` INT,
  `lCurImportance` INT,
  `mReplacements` LONGTEXT,
  `lHobjChartFilter` INT,
  `tblChartFilter` INT,
  `lAge` INT,
  PRIMARY KEY (`hadv`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `ADV_SUM` (
  `lAdvId` INT NOT NULL,
  `dtLatest` DATETIME,
  `cAdv` INT,
  PRIMARY KEY (`lAdvId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `Advisor Important Dates Custom Pool` (
  `hAdvisor Important Dates Custom Pool` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `fChecked` BOOLEAN NOT NULL DEFAULT 0,
  `szEvent` VARCHAR(255),
  `dtDate` DATETIME,
  `dtLastTrgrd` DATETIME,
  PRIMARY KEY (`hAdvisor Important Dates Custom Pool`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `Asset Allocation Custom Pool` (
  `hAsset Allocation Custom Pool` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `Object to change` INT,
  `Object Type` INT,
  `Total value` DECIMAL(19,4),
  PRIMARY KEY (`hAsset Allocation Custom Pool`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `AUTO` (
  `hauto` INT NOT NULL,
  `nYear` INT,
  `szLic` VARCHAR(10),
  `szMake` VARCHAR(32),
  `dtPutToBiz` DATETIME,
  `szModel` VARCHAR(32),
  `dtSerial` DATETIME,
  `lHacct` INT,
  `autot` INT,
  `amtCur` DECIMAL(19,4),
  `dtWarrantyEnd` DATETIME,
  `mVIN` LONGTEXT,
  `mWarranty` LONGTEXT,
  `mURLMfg` LONGTEXT,
  `mPurchaseLoc` LONGTEXT,
  PRIMARY KEY (`hauto`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `AWD` (
  `hawd` INT NOT NULL,
  `hpgm` INT,
  `lHacct` INT,
  `awdt` INT,
  `dt` DATETIME,
  `dUnits` DOUBLE,
  `mMemo` LONGTEXT,
  `fExpireEntered` BOOLEAN NOT NULL DEFAULT 0,
  `dtSerial` DATETIME,
  `dtExpire` DATETIME,
  PRIMARY KEY (`hawd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `BGT` (
  `hbgt` INT NOT NULL,
  `szFull` VARCHAR(64) COLLATE utf8mb4_bin,
  `bf` INT,
  `dtStart` DATETIME,
  `dtEnd` DATETIME,
  `dtSerial` DATETIME,
  `frq` INT,
  `cFrqInst` DOUBLE,
  `fSimple` BOOLEAN NOT NULL DEFAULT 0,
  `grfBgtStep` INT,
  PRIMARY KEY (`hbgt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `BGT_BKT` (
  `hbgtbkt` INT NOT NULL,
  `bbt` INT,
  `szFull` VARCHAR(64),
  `dtSerial` DATETIME,
  `hbgt` INT,
  PRIMARY KEY (`hbgtbkt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `BGT_ITM` (
  `hbgtitm` INT NOT NULL,
  `hbgt` INT,
  `hcat` INT,
  `szFull` VARCHAR(80),
  `bif` INT,
  `amtPerFrq` DECIMAL(19,4),
  `amt1` DECIMAL(19,4),
  `amt2` DECIMAL(19,4),
  `amt3` DECIMAL(19,4),
  `amt4` DECIMAL(19,4),
  `amt5` DECIMAL(19,4),
  `amt6` DECIMAL(19,4),
  `amt7` DECIMAL(19,4),
  `amt8` DECIMAL(19,4),
  `amt9` DECIMAL(19,4),
  `amt10` DECIMAL(19,4),
  `amt11` DECIMAL(19,4),
  `amt12` DECIMAL(19,4),
  `dtSerial` DATETIME,
  `hbgtbkt` INT,
  `frq` INT,
  `cFrqInst` DOUBLE,
  `dt` DATETIME,
  `hbgtitmLink` INT,
  PRIMARY KEY (`hbgtitm`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `BILL` (
  `hbill` INT NOT NULL,
  `st` INT,
  `frq` INT,
  `cFrqInst` DOUBLE,
  `dt` DATETIME,
  `iinstNextUnpaid` INT,
  `dtMax` DATETIME,
  `cInstMax` INT,
  `dtSerial` DATETIME,
  `itrnLink` INT,
  `hbillHead` INT,
  `iinst` INT,
  `itrn` INT,
  `lHtrn` INT,
  `iinstLastSkipped` INT,
  PRIMARY KEY (`hbill`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `BILL_FLD` (
  `hbill` INT NOT NULL,
  `fld` INT NOT NULL,
  `vt` INT,
  `rgbVal` LONGBLOB,
  `dtSerial` DATETIME,
  `szDbgCol` VARCHAR(64),
  `szDbgVal` VARCHAR(128),
  PRIMARY KEY (`hbill`, `fld`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `CAT` (
  `hcat` INT NOT NULL,
  `hct` INT,
  `nLevel` INT,
  `hcatParent` INT,
  `szAls` VARCHAR(8),
  `szFull` VARCHAR(48),
  `mComment` LONGTEXT,
  `fCalc` BOOLEAN NOT NULL DEFAULT 0,
  `amtTotal` DECIMAL(19,4),
  `fTax` BOOLEAN NOT NULL DEFAULT 0,
  `dVat` DOUBLE,
  `lConcept` INT,
  `lType` INT,
  `fBusiness` BOOLEAN NOT NULL DEFAULT 0,
  `dtSerial` DATETIME,
  `fAdvChart` BOOLEAN NOT NULL DEFAULT 0,
  `fAdvOvr` BOOLEAN NOT NULL DEFAULT 0,
  `amtAdvOvr` DECIMAL(19,4),
  `fAdvBdgt` BOOLEAN NOT NULL DEFAULT 0,
  `fAdvClose` BOOLEAN NOT NULL DEFAULT 0,
  PRIMARY KEY (`hcat`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `CESRC` (
  `ceobjt` INT NOT NULL,
  `lHobj` INT NOT NULL,
  `ceoId` INT,
  PRIMARY KEY (`ceobjt`, `lHobj`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `CLI` (
  `hcli` INT NOT NULL,
  `szFull` VARCHAR(50),
  PRIMARY KEY (`hcli`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `CLI_DAT` (
  `hclidat` INT NOT NULL,
  `hcli` INT,
  `idData` INT,
  `oft` INT,
  `rgbVal` LONGBLOB,
  `fVal` BOOLEAN NOT NULL DEFAULT 0,
  `lVal` INT,
  `dVal` DOUBLE,
  `amtVal` DECIMAL(19,4),
  `dtVal` DATETIME,
  PRIMARY KEY (`hclidat`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `CNTRY` (
  `hcntry` INT NOT NULL,
  `szFull` VARCHAR(32),
  `szCode` VARCHAR(2),
  `hcrncDef` INT,
  `fEuroConvertible` BOOLEAN NOT NULL DEFAULT 0,
  `fEuroConverted` BOOLEAN NOT NULL DEFAULT 0,
  `fOnlineEnabled` BOOLEAN NOT NULL DEFAULT 0,
  `dtSerial` DATETIME,
  PRIMARY KEY (`hcntry`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `CRIT` (
  `hcrit` INT NOT NULL,
  `hview` INT,
  `ifc` INT,
  `bop` INT,
  `hitm` INT,
  `critt` INT,
  `cop` INT,
  `hviewSub` INT,
  `vart` INT,
  `rgbVar` LONGBLOB,
  PRIMARY KEY (`hcrit`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `CRNC` (
  `hcrnc` INT NOT NULL,
  `szName` VARCHAR(32),
  `lcid` INT,
  `rgbFormat` LONGBLOB,
  `szIsoCode` VARCHAR(8),
  `szSymbol` VARCHAR(12),
  `fOnline` BOOLEAN NOT NULL DEFAULT 0,
  `dtSerial` DATETIME,
  PRIMARY KEY (`hcrnc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `CRNC_EXCHG` (
  `hcrncFrom` INT NOT NULL,
  `hcrncTo` INT NOT NULL,
  `rate` DOUBLE,
  `dt` DATETIME,
  `fReversed` BOOLEAN NOT NULL DEFAULT 0,
  `fThroughEuro` BOOLEAN NOT NULL DEFAULT 0,
  `exchgid` INT,
  `fHist` BOOLEAN NOT NULL DEFAULT 0,
  `szSymbol` VARCHAR(32),
  PRIMARY KEY (`hcrncFrom`, `hcrncTo`, `fHist`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `CT` (
  `hct` INT NOT NULL,
  `szFull` VARCHAR(32),
  `fldTrn` INT,
  `szFullAlt` VARCHAR(32),
  `ctid` INT,
  `dtSerial` DATETIME,
  PRIMARY KEY (`hct`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `DHD` (
  `dhd` INT NOT NULL,
  `wMagic` INT,
  `cchDec` INT,
  `szBackup` VARCHAR(80),
  `lcid` INT,
  `hcatServiceLast` INT,
  `hcatIntEarnLast` INT,
  `hcatIntPaidLast` INT,
  `hcatOpenAdjLast` INT,
  `hcatEndAdjLast` INT,
  `hcrncCur` INT,
  `grfVersion` INT,
  `ast` INT,
  `olidMac` INT,
  `hcrncDef` INT,
  `hfiLast` INT,
  `hcatCG` INT,
  `hcatIntTax` INT,
  `hcatIntNoTax` INT,
  `hcatDiv` INT,
  `hcatCGDist` INT,
  `hcatESO` INT,
  `mguidFile` LONGTEXT,
  `dtExpire` DATETIME,
  `rgbNhdata` LONGBLOB,
  PRIMARY KEY (`dhd`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `FI` (
  `hfi` INT NOT NULL,
  `szFull` VARCHAR(255),
  `haddr` INT,
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `mComment` LONGTEXT,
  `szBdsId` LONGTEXT,
  `fit` INT,
  `fOEMDisabled` BOOLEAN NOT NULL DEFAULT 0,
  `mActiveStmtURL` LONGTEXT,
  `mBankAdPath` LONGTEXT,
  `mBankAdURL` LONGTEXT,
  `mTradingURL` LONGTEXT,
  `haddrPOL` INT,
  `mNewsURL` LONGTEXT,
  `mWebPayURL` LONGTEXT,
  `mWebSignupURL` LONGTEXT,
  `mWebServicesURL` LONGTEXT,
  `mSetupFileURL` LONGTEXT,
  `dtSerial` DATETIME,
  `fDisallowThirdParty` BOOLEAN NOT NULL DEFAULT 0,
  PRIMARY KEY (`hfi`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `Goal Custom Pool` (
  `hGoal Custom Pool` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `rgbGoalObj` LONGBLOB,
  PRIMARY KEY (`hGoal Custom Pool`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `Inventory Custom Pool` (
  `hInventory Custom Pool` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `szLocation` VARCHAR(255),
  `szMake` VARCHAR(255),
  `szSerial` VARCHAR(255),
  `szStore` VARCHAR(255),
  `szNote` VARCHAR(255),
  `dt` DATETIME,
  `cyPrice` DECIMAL(19,4),
  `cyReplace` DECIMAL(19,4),
  `cyValue` DECIMAL(19,4),
  `lType` INT,
  `Phone` VARCHAR(255),
  `dtWrrntExp` DATETIME,
  `Web` VARCHAR(255),
  `szWrrntDtls` VARCHAR(255),
  `szImgStore` VARCHAR(255),
  PRIMARY KEY (`hInventory Custom Pool`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `ITM` (
  `hitm` INT NOT NULL,
  `hcli` INT,
  `lt` INT,
  `ft` INT,
  `fr` INT,
  `szName` VARCHAR(64),
  `tblPool` INT,
  `tblPhys` INT,
  `icolPhys` INT,
  `lMatch` INT,
  `tblFrom` INT,
  `icolFrom` INT,
  `tblSrc` INT,
  `grfopt` INT,
  `mIndex` LONGTEXT,
  `rgbStats` LONGBLOB,
  PRIMARY KEY (`hitm`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `IVTY` (
  `hivty` INT NOT NULL,
  `hprod` INT,
  `ivtyt` INT,
  `dt` DATETIME,
  `dQty` DOUBLE,
  `dPrice` DOUBLE,
  `mMemo` LONGTEXT,
  `dtSerial` DATETIME,
  PRIMARY KEY (`hivty`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `LOT` (
  `hlot` INT NOT NULL,
  `htrnBuy` INT,
  `htrnSell` INT,
  `qty` DOUBLE,
  `hacct` INT,
  `hsec` INT,
  `dtBuy` DATETIME,
  `dtSell` DATETIME,
  `hlotOpen` INT,
  `hlotLink` INT,
  `lott` INT,
  `htrnOpen` INT,
  `htrnClose` INT,
  `dtOpen` DATETIME,
  `dtClose` DATETIME,
  `dtSerial` DATETIME,
  PRIMARY KEY (`hlot`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `LSTEP` (
  `hstep` INT NOT NULL,
  `hacctLoan` INT,
  `hstepNext` INT,
  `dtEnd` DATETIME,
  `dtAmortEnd` DATETIME,
  `dRateCalc` DOUBLE,
  `dRateUser` DOUBLE,
  `amtPI` DECIMAL(19,4),
  `amtPayment` DECIMAL(19,4),
  `dtSerial` DATETIME,
  PRIMARY KEY (`hstep`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `MAIL` (
  `hmail` INT NOT NULL,
  `mt` INT,
  `fToFi` BOOLEAN NOT NULL DEFAULT 0,
  `olst` INT,
  `mTo` LONGTEXT,
  `mFrom` LONGTEXT,
  `mSubject` LONGTEXT,
  `mBody` LONGTEXT,
  `hacct` INT,
  `ltrn` INT,
  `hfi` INT,
  `dtSerial` DATETIME,
  `fRead` BOOLEAN NOT NULL DEFAULT 0,
  `dtSentByFi` DATETIME,
  PRIMARY KEY (`hmail`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `MCSRC` (
  `hmcsrc` INT NOT NULL,
  `objt` INT,
  `lHobj` INT,
  `mcoId` INT,
  `fDeleted` BOOLEAN NOT NULL DEFAULT 0,
  `dtDeleted` DATETIME,
  `mcoIdAcct` INT,
  `mcoIdSoq` INT,
  PRIMARY KEY (`hmcsrc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `PAY` (
  `hpay` INT NOT NULL,
  `hpayParent` INT,
  `haddr` INT,
  `mComment` LONGTEXT,
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `szAls` VARCHAR(8),
  `szFull` VARCHAR(64),
  `mAcctNum` LONGTEXT,
  `mBankId` LONGTEXT,
  `mBranchId` LONGTEXT,
  `mUserAcctAtPay` LONGTEXT,
  `mIntlChkSum` LONGTEXT,
  `mCompanyName` LONGTEXT,
  `mContact` LONGTEXT,
  `haddrBill` INT,
  `haddrShip` INT,
  `mCellPhone` LONGTEXT,
  `mPager` LONGTEXT,
  `mWebPage` LONGTEXT,
  `terms` INT,
  `mPmtType` LONGTEXT,
  `mCCNum` LONGTEXT,
  `dtCCExp` DATETIME,
  `dDiscount` DOUBLE,
  `dRateTax` DOUBLE,
  `fVendor` BOOLEAN NOT NULL DEFAULT 0,
  `fCust` BOOLEAN NOT NULL DEFAULT 0,
  `rgbEntryId` LONGBLOB,
  `dtLastModified` DATETIME,
  `lContactData` INT,
  `shippref` INT,
  `fNoRecurringBill` BOOLEAN NOT NULL DEFAULT 0,
  `dtSerial` DATETIME,
  `grfcontt` INT,
  `fAutofillMemo` BOOLEAN NOT NULL DEFAULT 0,
  `dtLast` DATETIME,
  `rgbMemos` LONGBLOB,
  PRIMARY KEY (`hpay`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `PGM` (
  `hpgm` INT NOT NULL,
  `pgmt` INT,
  `szFull` VARCHAR(64),
  `pgc` INT,
  `unitt` INT,
  `fClosed` BOOLEAN NOT NULL DEFAULT 0,
  `dPeriodExpire` DOUBLE,
  `mNum` LONGTEXT,
  `mMemo` LONGTEXT,
  `mURL` LONGTEXT,
  `rgbTerms` LONGBLOB,
  `dtSerial` DATETIME,
  `fDetectedExp` BOOLEAN NOT NULL DEFAULT 0,
  PRIMARY KEY (`hpgm`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `PMT` (
  `hpmt` INT NOT NULL,
  `hcust` INT,
  `htrnInvoice` INT,
  `htrnPmt` INT,
  `dtInvoice` DATETIME,
  `dtPmt` DATETIME,
  `amt` DECIMAL(19,4),
  `dtSerial` DATETIME,
  PRIMARY KEY (`hpmt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `PORT_REC` (
  `hportrec` INT NOT NULL,
  `hacct` INT,
  `hsec` INT,
  `fPostponed` BOOLEAN NOT NULL DEFAULT 0,
  `fSelected` BOOLEAN NOT NULL DEFAULT 0,
  `amtUser` DECIMAL(19,4),
  `amtEmplyer` DECIMAL(19,4),
  `amtDividend` DECIMAL(19,4),
  `amtInterest` DECIMAL(19,4),
  `amt` DECIMAL(19,4),
  `dPrice` DOUBLE,
  `dQty` DOUBLE,
  `fFract` BOOLEAN NOT NULL DEFAULT 0,
  `dtSerial` DATETIME,
  PRIMARY KEY (`hportrec`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `Portfolio View Custom Pool` (
  `hPortfolio View Custom Pool` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `lDefinedView` INT,
  `lGroupType` INT,
  `lSortField` INT,
  `hviewAcctSelect` INT,
  `rbgHeaderCollapesState` LONGBLOB,
  `rgbAcctSorting` LONGBLOB,
  `fAscending` BOOLEAN NOT NULL DEFAULT 0,
  `rgbPortFieldInfo` LONGBLOB,
  `cbPortFieldInfo` INT,
  `rgbFieldWidth` LONGBLOB,
  PRIMARY KEY (`hPortfolio View Custom Pool`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `POS_STMT` (
  `hposstmt` INT NOT NULL,
  `hacct` INT,
  `mUIDType` LONGTEXT,
  `mUID` LONGTEXT,
  `szFull` VARCHAR(70),
  `dUnitPrice` DOUBLE,
  `szSymbol` VARCHAR(32),
  `sct` INT,
  `qty` DOUBLE,
  `amtPar` DECIMAL(19,4),
  `dtMaturity` DATETIME,
  `fProcessed` BOOLEAN NOT NULL DEFAULT 0,
  `dtAsOf` DATETIME,
  `dtSerial` DATETIME,
  PRIMARY KEY (`hposstmt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `PRODUCT` (
  `hproduct` INT NOT NULL,
  `szFull` VARCHAR(32),
  `hcat` INT,
  `dPrice` DOUBLE,
  `fService` BOOLEAN NOT NULL DEFAULT 0,
  `mDesc` LONGTEXT,
  `dCost` DOUBLE,
  `fTaxable` BOOLEAN NOT NULL DEFAULT 0,
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `fTrackInv` BOOLEAN NOT NULL DEFAULT 0,
  `dtSerial` DATETIME,
  `szPartNum` VARCHAR(20),
  PRIMARY KEY (`hproduct`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `PROJ` (
  `hproj` INT NOT NULL,
  `szFull` VARCHAR(32),
  `dtStart` DATETIME,
  `dtEnd` DATETIME,
  `mDesc` LONGTEXT,
  `dtSerial` DATETIME,
  PRIMARY KEY (`hproj`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `PROV_FI` (
  `hprovfi` INT NOT NULL,
  `hfi` INT,
  `fCallInProgress` BOOLEAN NOT NULL DEFAULT 0,
  `haddr` INT,
  `mProvBankId` LONGTEXT,
  `fNotify` BOOLEAN NOT NULL DEFAULT 0,
  `mProvUserId` LONGTEXT,
  `dtLastCall` DATETIME,
  `mPayeeSyncToken` LONGTEXT,
  `mMailSyncToken` LONGTEXT,
  `dtLastAcctUp` DATETIME,
  `fStandAloneBillpay` BOOLEAN NOT NULL DEFAULT 0,
  `fPinChg` BOOLEAN NOT NULL DEFAULT 0,
  `mFileId` LONGTEXT,
  `mSessId` LONGTEXT,
  `grfProvCap` INT,
  `grfBankCap` INT,
  `grfPmtCap` INT,
  `grfInvCap` INT,
  `cPinMin` INT,
  `cPinMax` INT,
  `cDfltDaysWith` INT,
  `cDfltDaysToPay` INT,
  `cIntraDfltDaysWith` INT,
  `cIntraDfltDaysToPay` INT,
  `cXferDfltDaysWith` INT,
  `cXferDfltDaysToPay` INT,
  `cIdDfltDaysWith` INT,
  `cIdDfltDaysToPay` INT,
  `mFiURL` LONGTEXT,
  `mShortName` LONGTEXT,
  `mInfoHtmlPath` LONGTEXT,
  `mSignupURL` LONGTEXT,
  `mSetupFileURL` LONGTEXT,
  `mPmtMailSyncToken` LONGTEXT,
  `mTan` LONGTEXT,
  `mFileUID` LONGTEXT,
  `rgbSendFileBackup` LONGBLOB,
  `rgbReceiveFileBackup` LONGBLOB,
  `mBrokerId` LONGTEXT,
  `mAcctSyncToken` LONGTEXT,
  `fSelected` BOOLEAN NOT NULL DEFAULT 0,
  `dtSerial` DATETIME,
  `szFull` VARCHAR(255),
  `fInternet` BOOLEAN NOT NULL DEFAULT 0,
  `szVersion` VARCHAR(8),
  `szSubstituteCommName` VARCHAR(8),
  `mclsidSubstituteComm` LONGTEXT,
  `oldt` INT,
  `dtIniEntry` DATETIME,
  `dtMnnFile` DATETIME,
  `fOSURequired` BOOLEAN NOT NULL DEFAULT 0,
  `mUIDLabel` LONGTEXT,
  `mPwdLabel` LONGTEXT,
  `lIniGuid` INT,
  `lBdsProvGuid` INT,
  `rgbPca` LONGBLOB,
  `fSavePinSuggested` BOOLEAN NOT NULL DEFAULT 0,
  `rgbCallSumm` LONGBLOB,
  `fStorePin` BOOLEAN NOT NULL DEFAULT 0,
  `fAlsRequired` BOOLEAN NOT NULL DEFAULT 0,
  `frqUpd` INT,
  `cFrqInstUpd` DOUBLE,
  `dtLastAttempt` DATETIME,
  `rgbLogoL` LONGBLOB,
  `rgbLogoS` LONGBLOB,
  PRIMARY KEY (`hprovfi`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `PROV_FI_PAY` (
  `hprovpayfi` INT NOT NULL,
  `hprovfi` INT,
  `hpay` INT,
  `hfi` INT,
  `haddr` INT,
  `fNotify` BOOLEAN NOT NULL DEFAULT 0,
  `fNew` BOOLEAN NOT NULL DEFAULT 0,
  `mPayID` LONGTEXT,
  `cdayToPayPre` INT,
  `cdayToPayPost` INT,
  `rgbAnon` LONGBLOB,
  `mPayListId` LONGTEXT,
  `pmtt` INT,
  `dtSerial` DATETIME,
  PRIMARY KEY (`hprovpayfi`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `Report Custom Pool` (
  `hReport Custom Pool` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `rptType` INT,
  `rptSub` INT,
  `tvTrans` INT,
  `rgbSRPT` LONGBLOB,
  `szTitle` VARCHAR(64),
  `grfView` INT,
  `rglColWidth` LONGBLOB,
  `rgbMetaFile` LONGBLOB,
  PRIMARY KEY (`hReport Custom Pool`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `SAV_GOAL` (
  `hsavgoal` INT NOT NULL,
  `szFull` VARCHAR(64),
  `dt` DATETIME,
  `amt` DECIMAL(19,4),
  `dtSerial` DATETIME,
  `hbgt` INT,
  PRIMARY KEY (`hsavgoal`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `SEC` (
  `hsec` INT NOT NULL,
  `szExchg` VARCHAR(32),
  `szFull` VARCHAR(70),
  `szSymbol` VARCHAR(32),
  `mComment` LONGTEXT,
  `amt` DECIMAL(19,4),
  `dtMaturity` DATETIME,
  `dtSplit` DATETIME,
  `sct` INT,
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `fTaxExempt` BOOLEAN NOT NULL DEFAULT 0,
  `sic` INT,
  `fOLQuotes` BOOLEAN NOT NULL DEFAULT 0,
  `mUIDType` LONGTEXT,
  `mUID` LONGTEXT,
  `dtLastHistQuote` DATETIME,
  `fGetHistQuotes` BOOLEAN NOT NULL DEFAULT 0,
  `d52WeekHigh` DOUBLE,
  `d52WeekLow` DOUBLE,
  `dSharesOutstanding` DOUBLE,
  `cbt` INT,
  `hcrnc` INT,
  `hcntry` INT,
  `fEuroPriceDownloaded` BOOLEAN NOT NULL DEFAULT 0,
  `fEuroDialogDisabled` BOOLEAN NOT NULL DEFAULT 0,
  `fWatch` BOOLEAN NOT NULL DEFAULT 0,
  `dCapitalization` DOUBLE,
  `hsecLink` INT,
  `dIntRate` DOUBLE,
  `frqIntPmt` INT,
  `cFrqInstIntPmt` DOUBLE,
  `sctsub` INT,
  `rating` INT,
  `dtCall` DATETIME,
  `szIssuer` VARCHAR(32),
  `dtLastUpdate` DATETIME,
  `dtFYI` DATETIME,
  `dBid` DOUBLE,
  `dAsk` DOUBLE,
  `dBeta` DOUBLE,
  `dPercentCash` DOUBLE,
  `dPercentEquity` DOUBLE,
  `dPercentDebt` DOUBLE,
  `dPercentOther` DOUBLE,
  `dDividendYield` DOUBLE,
  `dTargetHigh` DOUBLE,
  `dTargetLow` DOUBLE,
  `amtEPS` DECIMAL(19,4),
  `size` INT,
  `dtFYILastRead` DATETIME,
  `dtNewsLastRead` DATETIME,
  `dtNews` DATETIME,
  `sat` INT,
  `dPercentStockS` DOUBLE,
  `dPercentStockM` DOUBLE,
  `dPercentStockL` DOUBLE,
  `fViewInCrnc` BOOLEAN NOT NULL DEFAULT 0,
  `dtSerial` DATETIME,
  `mCusip` LONGTEXT,
  `mNewsURL` LONGTEXT,
  `dPctVestFirst` DOUBLE,
  `dPctVestNext` DOUBLE,
  `dPeriodExpire` DOUBLE,
  `dPeriodVestNext` DOUBLE,
  `dVestFirst` DOUBLE,
  `fAdvHigh` BOOLEAN NOT NULL DEFAULT 0,
  `amtAdvHigh` DOUBLE,
  `fAdvLow` BOOLEAN NOT NULL DEFAULT 0,
  `amtAdvLow` DOUBLE,
  `dAdvLast` DOUBLE,
  PRIMARY KEY (`hsec`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `SEC_SPLIT` (
  `hss` INT NOT NULL,
  `cshrPre` INT,
  `cshrPost` INT,
  `dtRecord` DATETIME,
  `dPriceSplit` DOUBLE,
  `fFractSplit` BOOLEAN NOT NULL DEFAULT 0,
  PRIMARY KEY (`hss`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `SIC` (
  `sic` INT NOT NULL,
  `hcat` INT,
  PRIMARY KEY (`sic`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `SOQ` (
  `hsoq` INT NOT NULL,
  `hsec` INT,
  `hacct` INT,
  `dQty` DOUBLE,
  `mcoId` INT,
  `dtSerial` DATETIME,
  `fUpdated` BOOLEAN NOT NULL DEFAULT 0,
  PRIMARY KEY (`hsoq`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `SP` (
  `hsp` INT NOT NULL,
  `dt` DATETIME,
  `hsec` INT,
  `src` INT,
  `dPrice` DOUBLE,
  `fFractPrice` BOOLEAN NOT NULL DEFAULT 0,
  `dStrike` DOUBLE,
  `fFractStrike` BOOLEAN NOT NULL DEFAULT 0,
  `dOpen` DOUBLE,
  `fFractOpen` BOOLEAN NOT NULL DEFAULT 0,
  `dHigh` DOUBLE,
  `fFractHigh` BOOLEAN NOT NULL DEFAULT 0,
  `dLow` DOUBLE,
  `fFractLow` BOOLEAN NOT NULL DEFAULT 0,
  `dPE` DOUBLE,
  `fFractPE` BOOLEAN NOT NULL DEFAULT 0,
  `dtExpire` DATETIME,
  `fClosing` BOOLEAN NOT NULL DEFAULT 0,
  `vol` INT,
  `hss` INT,
  `dChange` DOUBLE,
  `fFractChange` BOOLEAN NOT NULL DEFAULT 0,
  `dtSerial` DATETIME,
  `exchgid` INT,
  PRIMARY KEY (`hsp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `STMT` (
  `hstmt` INT NOT NULL,
  `hacct` INT,
  `atTo` INT,
  `amtLastStmt` DECIMAL(19,4),
  `mBranchIdTo` LONGTEXT,
  `mBankIdTo` LONGTEXT,
  `mPayId` LONGTEXT,
  `dtPost` DATETIME,
  `szNamePay` VARCHAR(64),
  `amt` DECIMAL(19,4),
  `mMemo` LONGTEXT,
  `mProvId` LONGTEXT,
  `oltt` INT,
  `szId` VARCHAR(12),
  `mFiStmtId` LONGTEXT,
  `sic` INT,
  `cs` INT,
  `frq` INT,
  `cFrqInst` DOUBLE,
  `cpmtsOL` INT,
  `mApayModelId` LONGTEXT,
  `mUserAcctAtPay` LONGTEXT,
  `mClientId` LONGTEXT,
  `haddr` INT,
  `stmtt` INT,
  `sct` INT,
  `dUnitPrice` DOUBLE,
  `mUIDType` LONGTEXT,
  `szSecName` VARCHAR(70),
  `mUID` LONGTEXT,
  `szSecSymbol` VARCHAR(32),
  `dtTrade` DATETIME,
  `act` INT,
  `amtCommission` DECIMAL(19,4),
  `inct` INT,
  `lott` INT,
  `cshrPre` INT,
  `cshrPost` INT,
  `cshrNumerator` INT,
  `cshrDenominator` INT,
  `dtSettle` DATETIME,
  `qty` DOUBLE,
  `amtPar` DECIMAL(19,4),
  `dtMaturity` DATETIME,
  `mIntlChkSumTo` LONGTEXT,
  `grfEbppFlags` INT,
  `mEbppPmtId` LONGTEXT,
  `mNumAcctTo` LONGTEXT,
  `lHpayRel` INT,
  `dtSerial` DATETIME,
  `lHcrncUser` INT,
  `amtUser` DECIMAL(19,4),
  `amtCmnUser` DECIMAL(19,4),
  `amtParUser` DECIMAL(19,4),
  `amtLastStmtUser` DECIMAL(19,4),
  `dRateToBase` DOUBLE,
  PRIMARY KEY (`hstmt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `SVC` (
  `hsvc` INT NOT NULL,
  `hacct` INT,
  `hprovfi` INT,
  `grfsvcCurrent` INT,
  `grfsvcOld` INT,
  `grfsvcPending` INT,
  `fNotify` BOOLEAN NOT NULL DEFAULT 0,
  `fNew` BOOLEAN NOT NULL DEFAULT 0,
  `rgbAnonAcct` LONGBLOB,
  `amtLastStmt` DECIMAL(19,4),
  `dtLastStmtDown` DATETIME,
  `olat` INT,
  `mAcctNumAlias` LONGTEXT,
  `mBankId` LONGTEXT,
  `mBranchId` LONGTEXT,
  `mIntlChkSum` LONGTEXT,
  `dtSerial` DATETIME,
  `mStmtSyncToken` LONGTEXT,
  `mIntraSyncToken` LONGTEXT,
  `mBankmailSyncToken` LONGTEXT,
  `mPmtSyncToken` LONGTEXT,
  `mRecPmtSyncToken` LONGTEXT,
  `mInvMailSyncToken` LONGTEXT,
  PRIMARY KEY (`hsvc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `Tax Rate Custom Pool` (
  `hTax Rate Custom Pool` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `Type` INT,
  `damtStdDed` DOUBLE,
  `damtLow1` DOUBLE,
  `damtLow2` DOUBLE,
  `damtLow3` DOUBLE,
  `damtLow4` DOUBLE,
  `damtLow5` DOUBLE,
  `damtHigh1` DOUBLE,
  `damtHigh2` DOUBLE,
  `damtHigh3` DOUBLE,
  `damtHigh4` DOUBLE,
  `dRate1` DOUBLE,
  `dRate2` DOUBLE,
  `dRate3` DOUBLE,
  `dRate4` DOUBLE,
  `dRate5` DOUBLE,
  `dRateCapGains` DOUBLE,
  `dRateCapGainsLowBraket` DOUBLE,
  `dRateCapGainsLongLongT` DOUBLE,
  `dRateCapGainsMT` DOUBLE,
  `damtStdEx` DOUBLE,
  `damtDedBlind` DOUBLE,
  `damtDedOver65` DOUBLE,
  `damtThreshDed` DOUBLE,
  `damtThreshExemp` DOUBLE,
  `damtMaxCapLoss` DOUBLE,
  PRIMARY KEY (`hTax Rate Custom Pool`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `TAXLINE` (
  `htaxline` INT NOT NULL,
  `txn` INT,
  `txcpy` INT,
  `fInclude` BOOLEAN NOT NULL DEFAULT 0,
  `tds` INT,
  `lHobjLine` INT,
  `dtSerial` DATETIME,
  PRIMARY KEY (`htaxline`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `TMI` (
  `htmi` INT NOT NULL,
  `hpay` INT,
  `hproduct` INT,
  `dtStart` DATETIME,
  `dtEnd` DATETIME,
  `dHours` DOUBLE,
  `dRate` DOUBLE,
  `mDesc` LONGTEXT,
  `dtSerial` DATETIME,
  PRIMARY KEY (`htmi`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `TRIP` (
  `htrip` INT NOT NULL,
  `hauto` INT,
  `dtStart` DATETIME,
  `dtEnd` DATETIME,
  `mDest` LONGTEXT,
  `mContact` LONGTEXT,
  `mPurpose` LONGTEXT,
  `iOdoStart` INT,
  `iOdoEnd` INT,
  `cOdoNet` INT,
  `tut` INT,
  `amtFees` DECIMAL(19,4),
  `dtSerial` DATETIME,
  PRIMARY KEY (`htrip`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `TRN` (
  `htrn` INT NOT NULL,
  `hacct` INT,
  `hacctLink` INT,
  `hpay` INT,
  `dt` DATETIME,
  `dtSent` DATETIME,
  `dtCleared` DATETIME,
  `dtPost` DATETIME,
  `cs` INT,
  `hsec` INT,
  `amt` DECIMAL(19,4),
  `szId` VARCHAR(13),
  `hcat` INT,
  `frq` INT,
  `fDefPmt` BOOLEAN NOT NULL DEFAULT 0,
  `mMemo` LONGTEXT,
  `oltt` INT,
  `grfEntryMethods` INT,
  `ps` INT,
  `amtVat` DECIMAL(19,4),
  `grftt` INT,
  `act` INT,
  `cFrqInst` DOUBLE,
  `fPrint` BOOLEAN NOT NULL DEFAULT 0,
  `mFiStmtId` LONGTEXT,
  `olst` INT,
  `fDebtPlan` BOOLEAN NOT NULL DEFAULT 0,
  `grfstem` INT,
  `cpmtsRemaining` INT,
  `instt` INT,
  `htrnSrc` INT,
  `payt` INT,
  `grftf` INT,
  `lHtxsrc` INT,
  `lHcrncUser` INT,
  `amtUser` DECIMAL(19,4),
  `amtVATUser` DECIMAL(19,4),
  `tef` INT,
  `fRefund` BOOLEAN NOT NULL DEFAULT 0,
  `fReimburse` BOOLEAN NOT NULL DEFAULT 0,
  `dtSerial` DATETIME,
  `fUpdated` BOOLEAN NOT NULL DEFAULT 0,
  `fCCPmt` BOOLEAN NOT NULL DEFAULT 0,
  `fDefBillAmt` BOOLEAN NOT NULL DEFAULT 0,
  `fDefBillDate` BOOLEAN NOT NULL DEFAULT 0,
  `lHclsKak` INT,
  `lHcls1` INT,
  `lHcls2` INT,
  `dtCloseOffYear` DATETIME,
  `dtOldRel` DATETIME,
  `hbillHead` INT,
  `iinst` INT,
  `amtBase` DECIMAL(19,4),
  `rt` INT,
  `amtPreRec` DECIMAL(19,4),
  `amtPreRecUser` DECIMAL(19,4),
  `hstmtRel` INT,
  `dRateToBase` DOUBLE,
  `fAVRLimitsApply` BOOLEAN NOT NULL DEFAULT 0,
  PRIMARY KEY (`htrn`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `TRN_INV` (
  `htrn` INT NOT NULL,
  `dPrice` DOUBLE,
  `qty` DOUBLE,
  `amtCmn` DECIMAL(19,4),
  `fFract` BOOLEAN NOT NULL DEFAULT 0,
  `lott` INT,
  `amtCmnUser` DECIMAL(19,4),
  `amtInt` DECIMAL(19,4),
  PRIMARY KEY (`htrn`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `TRN_INVOICE` (
  `htrn` INT NOT NULL,
  `dtShip` DATETIME,
  `dtDue` DATETIME,
  `terms` INT,
  `haddrShip` INT,
  `haddrBill` INT,
  `amtTotal` DECIMAL(19,4),
  `hproduct` INT,
  `qtyProd` DOUBLE,
  `dPriceProd` DOUBLE,
  `fEstimate` BOOLEAN NOT NULL DEFAULT 0,
  `fTax` BOOLEAN NOT NULL DEFAULT 0,
  `dRateTax` DOUBLE,
  `amtTax` DECIMAL(19,4),
  `mPurchOrder` LONGTEXT,
  `shippref` INT,
  `amtTotalUser` DECIMAL(19,4),
  `hproj` INT,
  `lHtrnReimburse` INT,
  `hivty` INT,
  `fTmiTrn` BOOLEAN NOT NULL DEFAULT 0,
  `htmi` INT,
  `fInheritAddr` BOOLEAN NOT NULL DEFAULT 0,
  PRIMARY KEY (`htrn`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `TRN_OL` (
  `htrn` INT NOT NULL,
  `cpmtsOL` INT,
  `dtDueToBiller` DATETIME,
  `dtSentByOLP` DATETIME,
  `mProvId` LONGTEXT,
  `mApayModelId` LONGTEXT,
  `mClientId` LONGTEXT,
  `mEbppModelId` LONGTEXT,
  `mEbppFitid` LONGTEXT,
  `mEbppImageURL` LONGTEXT,
  `dtModelStart` DATETIME,
  `dtModelEnd` DATETIME,
  `mEbppPmtId` LONGTEXT,
  `fCannotSend` BOOLEAN NOT NULL DEFAULT 0,
  PRIMARY KEY (`htrn`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `TRN_SPLIT` (
  `htrn` INT NOT NULL,
  `htrnParent` INT,
  `iSplit` INT,
  PRIMARY KEY (`htrn`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `TRN_XFER` (
  `htrnFrom` INT NOT NULL,
  `htrnLink` INT,
  PRIMARY KEY (`htrnFrom`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `TXSRC` (
  `htxsrc` INT NOT NULL,
  `srct` INT,
  `lHsrc` INT,
  `txn` INT,
  `txcpy` INT,
  `txpd` INT,
  `dtSerial` DATETIME,
  PRIMARY KEY (`htxsrc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `UIE` (
  `huiel` INT NOT NULL,
  `elt` INT NOT NULL,
  `theme` INT NOT NULL,
  `subelt` INT NOT NULL,
  `pos` INT NOT NULL,
  `grf` INT,
  `szItem` VARCHAR(32),
  `mDesc` LONGTEXT,
  `mLinkURL` LONGTEXT,
  `mImageURL` LONGTEXT,
  `tbl` INT,
  `lHobjRel` INT,
  `dtSerial` DATETIME,
  PRIMARY KEY (`huiel`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `UKSavings` (
  `hUKSavings` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `szName` VARCHAR(32),
  `lType` INT,
  `lBondType` INT,
  `lNSType` INT,
  `lISAType` INT,
  `dtStart` DATETIME,
  `dtEnd` DATETIME,
  `lTaxExempt` INT,
  `lPenalties` INT,
  `lPenalyNoticeNum` INT,
  `lPenaltyNoticeUnits` INT,
  `lDontWarnLimits` INT,
  `lLimitsApplyToAll` INT,
  `fLimitsTotal` BOOLEAN NOT NULL DEFAULT 0,
  `crcLimitsTotal` DECIMAL(19,4),
  `fLimitsYearly` BOOLEAN NOT NULL DEFAULT 0,
  `crcLimitsYearly` DECIMAL(19,4),
  `lLimitsTaxYear` INT,
  `fLimitsFirstYear` BOOLEAN NOT NULL DEFAULT 0,
  `crcLimitsFirstYear` DECIMAL(19,4),
  `fLimitsTotalISACash` BOOLEAN NOT NULL DEFAULT 0,
  `crcLimitsTotalISACash` DECIMAL(19,4),
  `fLimitsYearlyISACash` BOOLEAN NOT NULL DEFAULT 0,
  `crcLimitsYearlyISACash` DECIMAL(19,4),
  `lLimitsTaxYearISACash` INT,
  `fLimitsFirstYearISACash` BOOLEAN NOT NULL DEFAULT 0,
  `crcLimitsFirstYearISACash` DECIMAL(19,4),
  `fLimitsTotalISAEquities` BOOLEAN NOT NULL DEFAULT 0,
  `crcLimitsTotalISAEquities` DECIMAL(19,4),
  `fLimitsYearlyISAEquities` BOOLEAN NOT NULL DEFAULT 0,
  `crcLimitsYearlyISAEquities` DECIMAL(19,4),
  `lLimitsTaxYearISAEquities` INT,
  `fLimitsFirstYearISAEquities` BOOLEAN NOT NULL DEFAULT 0,
  `crcLimitsFirstYearISAEquities` DECIMAL(19,4),
  `fLimitsTotalISALife` BOOLEAN NOT NULL DEFAULT 0,
  `crcLimitsTotalISALife` DECIMAL(19,4),
  `fLimitsYearlyISALife` BOOLEAN NOT NULL DEFAULT 0,
  `crcLimitsYearlyISALife` DECIMAL(19,4),
  `lLimitsTaxYearISALife` INT,
  `fLimitsFirstYearISALife` BOOLEAN NOT NULL DEFAULT 0,
  `crcLimitsFirstYearISALife` DECIMAL(19,4),
  `dEstimateGrowth` DOUBLE,
  `dEstimateIncrease` DOUBLE,
  `crcEstimatePayment` DECIMAL(19,4),
  `crcEstimateBalance` DECIMAL(19,4),
  `dtEstimateEnd` DATETIME,
  `dtNextPay` DATETIME,
  `lPayFrequency` INT,
  `szTitle` VARCHAR(255),
  `crcEstimatedValue` DECIMAL(19,4),
  `hobjAccount` INT,
  `hobjISACashAccount` INT,
  `hobjISAEquitiesAccount` INT,
  `hobjISALifeAccount` INT,
  `dEstimateGrowthRate` DOUBLE,
  `crcBalanceAtEstimate` DECIMAL(19,4),
  PRIMARY KEY (`hUKSavings`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `UKWiz` (
  `hUKWiz` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `lVersion` INT,
  `fSaveOldPension` BOOLEAN NOT NULL DEFAULT 0,
  PRIMARY KEY (`hUKWiz`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `UKWizAddress` (
  `hUKWizAddress` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `szName` VARCHAR(50),
  `szAddress` VARCHAR(250),
  PRIMARY KEY (`hUKWizAddress`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `UKWizCompanyCar` (
  `hUKWizCompanyCar` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `szSlotName` VARCHAR(35),
  `lSlotNumber` INT,
  `lMarque` VARCHAR(20),
  `lModel` VARCHAR(20),
  `crcListPrice` DECIMAL(19,4),
  `fOver4Years` BOOLEAN NOT NULL DEFAULT 0,
  `crcAccessoriesPrice` DECIMAL(19,4),
  `lCompanyMileage` INT,
  `fReplacingPrivate` BOOLEAN NOT NULL DEFAULT 0,
  `fDataFromMoney` BOOLEAN NOT NULL DEFAULT 0,
  `fPrivatePetrol` BOOLEAN NOT NULL DEFAULT 0,
  `lPrivateCapacity` INT,
  `crcPurchasePrice` DECIMAL(19,4),
  `lPrivateMileage` INT,
  `crcRunningCost` DECIMAL(19,4),
  `crcBuyingContrib` DECIMAL(19,4),
  `crcRunningContrib` DECIMAL(19,4),
  `fSupplyPrivateFuel` BOOLEAN NOT NULL DEFAULT 0,
  `fCompanyPetrol` BOOLEAN NOT NULL DEFAULT 0,
  `lCompanyCapacity` INT,
  `fSalaryAlternative` BOOLEAN NOT NULL DEFAULT 0,
  `crcSalaryIncrease` DECIMAL(19,4),
  `crcGrossSalary` DECIMAL(19,4),
  `fOutOfSERPS` BOOLEAN NOT NULL DEFAULT 0,
  `lTaxRate` DECIMAL(19,4),
  PRIMARY KEY (`hUKWizCompanyCar`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `UKWizLoan` (
  `hUKWizLoan` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `szSlotName` VARCHAR(35),
  `lSlotNumber` INT,
  `lWhatFor` INT,
  `lType` INT,
  `fDoYouKnow` BOOLEAN NOT NULL DEFAULT 0,
  `lCostGridRows` INT,
  `szCostGridText1` VARCHAR(50),
  `szCostGridText2` VARCHAR(50),
  `szCostGridText3` VARCHAR(50),
  `szCostGridText4` VARCHAR(50),
  `szCostGridText5` VARCHAR(50),
  `szCostGridText6` VARCHAR(50),
  `szCostGridText7` VARCHAR(50),
  `szCostGridText8` VARCHAR(50),
  `szCostGridText9` VARCHAR(50),
  `szCostGridText10` VARCHAR(50),
  `szCostGridText11` VARCHAR(50),
  `szCostGridText12` VARCHAR(50),
  `szCostGridText13` VARCHAR(50),
  `szCostGridText14` VARCHAR(50),
  `szCostGridText15` VARCHAR(50),
  `szCostGridText16` VARCHAR(50),
  `szCostGridText17` VARCHAR(50),
  `szCostGridText18` VARCHAR(50),
  `szCostGridText19` VARCHAR(50),
  `szCostGridText20` VARCHAR(50),
  `crcCostGridAmount1` DECIMAL(19,4),
  `crcCostGridAmount2` DECIMAL(19,4),
  `crcCostGridAmount3` DECIMAL(19,4),
  `crcCostGridAmount4` DECIMAL(19,4),
  `crcCostGridAmount5` DECIMAL(19,4),
  `crcCostGridAmount6` DECIMAL(19,4),
  `crcCostGridAmount7` DECIMAL(19,4),
  `crcCostGridAmount8` DECIMAL(19,4),
  `crcCostGridAmount9` DECIMAL(19,4),
  `crcCostGridAmount10` DECIMAL(19,4),
  `crcCostGridAmount11` DECIMAL(19,4),
  `crcCostGridAmount12` DECIMAL(19,4),
  `crcCostGridAmount13` DECIMAL(19,4),
  `crcCostGridAmount14` DECIMAL(19,4),
  `crcCostGridAmount15` DECIMAL(19,4),
  `crcCostGridAmount16` DECIMAL(19,4),
  `crcCostGridAmount17` DECIMAL(19,4),
  `crcCostGridAmount18` DECIMAL(19,4),
  `crcCostGridAmount19` DECIMAL(19,4),
  `crcCostGridAmount20` DECIMAL(19,4),
  `crcBalancing` DECIMAL(19,4),
  `crcDeposit` DECIMAL(19,4),
  `crcLoanAmount` DECIMAL(19,4),
  `crcBalloon` DECIMAL(19,4),
  `fCalcWhat` BOOLEAN NOT NULL DEFAULT 0,
  `crcMonthly` DECIMAL(19,4),
  `lLength` INT,
  `fYearsOrMonths` BOOLEAN NOT NULL DEFAULT 0,
  `crcInterest` DECIMAL(19,4),
  `crcResLength1` DECIMAL(19,4),
  `crcResMonthly1` DECIMAL(19,4),
  `crcResLoan1` DECIMAL(19,4),
  `crcResTotal1` DECIMAL(19,4),
  `crcResLength2` DECIMAL(19,4),
  `crcResMonthly2` DECIMAL(19,4),
  `crcResLoan2` DECIMAL(19,4),
  `crcResTotal2` DECIMAL(19,4),
  `crcResLength3` DECIMAL(19,4),
  `crcResMonthly3` DECIMAL(19,4),
  `crcResLoan3` DECIMAL(19,4),
  `crcResTotal3` DECIMAL(19,4),
  `crcResLength4` DECIMAL(19,4),
  `crcResMonthly4` DECIMAL(19,4),
  `crcResLoan4` DECIMAL(19,4),
  `crcResTotal4` DECIMAL(19,4),
  `crcResLength5` DECIMAL(19,4),
  `crcResMonthly5` DECIMAL(19,4),
  `crcResLoan5` DECIMAL(19,4),
  `crcResTotal5` DECIMAL(19,4),
  PRIMARY KEY (`hUKWizLoan`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `UKWizMortgage` (
  `hUKWizMortgage` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `szSlotName` VARCHAR(35),
  `lSlotNumber` INT,
  `fSharing` BOOLEAN NOT NULL DEFAULT 0,
  `lMarital` INT,
  `lPMarital` INT,
  `dtDOB` DATETIME,
  `dtPDOB` DATETIME,
  `crcGrossIncome` DECIMAL(19,4),
  `crcPGrossIncome` DECIMAL(19,4),
  `crcNGIncome` DECIMAL(19,4),
  `crcPNGIncome` DECIMAL(19,4),
  `fCCJ` BOOLEAN NOT NULL DEFAULT 0,
  `fPCCJ` BOOLEAN NOT NULL DEFAULT 0,
  `fHouse` BOOLEAN NOT NULL DEFAULT 0,
  `fLoanPayments` BOOLEAN NOT NULL DEFAULT 0,
  `fPLoanPayments` BOOLEAN NOT NULL DEFAULT 0,
  `crcLoanAmount` DECIMAL(19,4),
  `crcPLoanAmount` DECIMAL(19,4),
  `lLoanRemaining` INT,
  `lPLoanRemaining` INT,
  `crcFLiab` DECIMAL(19,4),
  `crcPFLiab` DECIMAL(19,4),
  `crcSellingPrice` DECIMAL(19,4),
  `crcOutLoan` DECIMAL(19,4),
  `crcEstateAgents` DECIMAL(19,4),
  `crcOtherFees` DECIMAL(19,4),
  `crcPurchasePrice` DECIMAL(19,4),
  `crcDeposit` DECIMAL(19,4),
  `crcPurchaseFees` DECIMAL(19,4),
  `lTerm` INT,
  `dIntRate` DOUBLE,
  `fDisMort` BOOLEAN NOT NULL DEFAULT 0,
  `lDisTerm` INT,
  `dDisRate` DOUBLE,
  `crcReportMortAmt` DECIMAL(19,4),
  `crcReportMirasAmt` DECIMAL(19,4),
  `crcReportPayments` DECIMAL(19,4),
  `crcReportRepayment` DECIMAL(19,4),
  `crcReportEndowment` DECIMAL(19,4),
  `crcReportPEP` DECIMAL(19,4),
  `crcReportPension` DECIMAL(19,4),
  PRIMARY KEY (`hUKWizMortgage`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `UKWizPenScheme` (
  `hUKWizPenScheme` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `lWSNumber` INT,
  `lType` INT,
  `szName` VARCHAR(35),
  `bCurrent` BOOLEAN NOT NULL DEFAULT 0,
  `bStakeholder` BOOLEAN NOT NULL DEFAULT 0,
  `dCBNPerc` DOUBLE,
  `dtDateJoin` DATETIME,
  `dAccRate` DOUBLE,
  `cyDefPen` DOUBLE,
  `dtExitDate` DATETIME,
  `lPenIncRate` INT,
  `dPenIncRateAmt` DOUBLE,
  `dSPProp` DOUBLE,
  `lSRA` INT,
  `dReduction` DOUBLE,
  `bCbnTyp` BOOLEAN NOT NULL DEFAULT 0,
  `cyCbnAmt` DOUBLE,
  `dtCbnStart` DATETIME,
  `dCbnInc` DOUBLE,
  `bDoYouKnow` BOOLEAN NOT NULL DEFAULT 0,
  `cyExistFund` DOUBLE,
  `dtFundDate` DATETIME,
  `dCh` DOUBLE,
  `dPCh` DOUBLE,
  `cyBSP` DOUBLE,
  `cyASP` DOUBLE,
  `dtStatrNIDate` DATETIME,
  `bFullNI` BOOLEAN NOT NULL DEFAULT 0,
  `dGap` INT,
  `dPost78Gap` INT,
  `bCONowInd` BOOLEAN NOT NULL DEFAULT 0,
  `dtCODate` DATETIME,
  `bCOPrevInd` BOOLEAN NOT NULL DEFAULT 0,
  `dtCODate2` DATETIME,
  `dtCIDate` DATETIME,
  PRIMARY KEY (`hUKWizPenScheme`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `UKWizPension` (
  `hUKWizPension` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `szSlotName` VARCHAR(35),
  `lSlotNumber` INT,
  `dtDOB` DATETIME,
  `fSex` BOOLEAN NOT NULL DEFAULT 0,
  `dGrossIncome` DOUBLE,
  `lRetirementAge` INT,
  `fExistingPension` BOOLEAN NOT NULL DEFAULT 0,
  `lGRBand` INT,
  `dSalInc` DOUBLE,
  `lPenInc` INT,
  `dFixedInc` DOUBLE,
  `dSpouse` DOUBLE,
  PRIMARY KEY (`hUKWizPension`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `UKWizWillExecutor` (
  `hUKWizWillExecutor` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `lWSNumber` INT,
  `szName` VARCHAR(35),
  `szAddress` VARCHAR(200),
  `szOccupation` VARCHAR(35),
  `szMessage` LONGTEXT,
  `lExecType` INT,
  PRIMARY KEY (`hUKWizWillExecutor`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `UKWizWillGift` (
  `hUKWizWillGift` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `lWSNumber` INT,
  `szDescription` VARCHAR(35),
  `szName` VARCHAR(35),
  `szAddress` VARCHAR(200),
  `lType` INT,
  `fIsInTrust` BOOLEAN NOT NULL DEFAULT 0,
  `fIsTaxFree` BOOLEAN NOT NULL DEFAULT 0,
  `szMessage` LONGTEXT,
  PRIMARY KEY (`hUKWizWillGift`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `UKWizWillGuardian` (
  `hUKWizWillGuardian` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `lWSNumber` INT,
  `szName` VARCHAR(35),
  `szAddress` VARCHAR(200),
  `fIsSole` BOOLEAN NOT NULL DEFAULT 0,
  `szDependent` VARCHAR(35),
  `szMessage` LONGTEXT,
  `lGuardType` INT,
  PRIMARY KEY (`hUKWizWillGuardian`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `UKWizWillLovedOne` (
  `hUKWizWillLovedOne` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `lWSNumber` INT,
  `szName` VARCHAR(35),
  `szAddress` VARCHAR(200),
  `fUsMyAddr` BOOLEAN NOT NULL DEFAULT 0,
  `szRelationship` VARCHAR(35),
  PRIMARY KEY (`hUKWizWillLovedOne`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `UKWizWillMaker` (
  `hUKWizWillMaker` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `lSlotNumber` INT,
  `szSlotName` VARCHAR(35),
  `lDays` INT,
  `fFamily` BOOLEAN NOT NULL DEFAULT 0,
  `fCheckedPlan` BOOLEAN NOT NULL DEFAULT 0,
  `lVerCreated` INT,
  PRIMARY KEY (`hUKWizWillMaker`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `UKWizWillPerson` (
  `hUKWizWillPerson` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `szName` VARCHAR(35),
  `szAddress` VARCHAR(200),
  `szOccupation` VARCHAR(35),
  PRIMARY KEY (`hUKWizWillPerson`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `UKWizWillResidue` (
  `hUKWizWillResidue` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `lWSNumber` INT,
  `lPercentage` INT,
  `szName` VARCHAR(35),
  `szAddress` VARCHAR(200),
  `szOccupation` VARCHAR(35),
  `fIsInTrust` BOOLEAN NOT NULL DEFAULT 0,
  `szMessage` LONGTEXT,
  `lBenifType` INT,
  PRIMARY KEY (`hUKWizWillResidue`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `UNOTE` (
  `hunote` INT NOT NULL,
  `nt` INT,
  `dtTarget` DATETIME NOT NULL,
  `szSubject` VARCHAR(64) NOT NULL,
  `mNote` LONGTEXT,
  `mNoteURL` LONGTEXT,
  `dTargetPrice` DOUBLE,
  `dtSerial` DATETIME,
  PRIMARY KEY (`hunote`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `VIEW` (
  `hview` INT NOT NULL,
  `so` INT,
  `grfflt` INT,
  `hcli` INT,
  `hitmPool` INT,
  `szFull` VARCHAR(32),
  `vart` INT,
  `rgbVar` LONGBLOB,
  `cfc` INT,
  `rgbEx` LONGBLOB,
  `dtSerial` DATETIME,
  PRIMARY KEY (`hview`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `Worksheet Custom Pool` (
  `hWorksheet Custom Pool` INT NOT NULL,
  `szFull` VARCHAR(64),
  `szAls` VARCHAR(16),
  `fHidden` BOOLEAN NOT NULL DEFAULT 0,
  `Type` INT,
  `rgbPrivate` LONGBLOB,
  PRIMARY KEY (`hWorksheet Custom Pool`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `XACCT` (
  `hacct` INT NOT NULL,
  `ctrnUnprinted` INT,
  `ctrnUnsent` INT,
  `amtBalance` DECIMAL(19,4),
  `szIdMost` VARCHAR(13),
  `htrnIdLast` INT,
  `cstmt` INT,
  PRIMARY KEY (`hacct`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `XBAG` (
  `hxbag` INT NOT NULL,
  `bt` INT,
  `tbl` INT,
  `lHobj` INT,
  `grf` INT,
  `dtFollowup` DATETIME,
  `szMemo` VARCHAR(255),
  `dtSerial` DATETIME,
  `dPct` DOUBLE,
  `lHobjRel` INT,
  `szId` VARCHAR(13),
  PRIMARY KEY (`hxbag`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `XMAPACCT` (
  `hxmapacct` INT NOT NULL,
  `hacct` INT,
  `hsvc` INT,
  `szNum` VARCHAR(128),
  `szBankId` VARCHAR(50),
  `mBranchId` LONGTEXT,
  `mIntlChkSum` LONGTEXT,
  PRIMARY KEY (`hxmapacct`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `XMAPSAT` (
  `hxmapsat` INT NOT NULL,
  `szMCCat` VARCHAR(255) NOT NULL,
  `sat` INT,
  PRIMARY KEY (`hxmapsat`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `XPAY` (
  `hpay` INT NOT NULL,
  `amtBal` DECIMAL(19,4),
  PRIMARY KEY (`hpay`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;

-- 2. Data: one transaction per table

COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;

-- 3. Secondary indexes

ALTER TABLE `ACCT`
  ADD KEY `AstTypeName` (`ast`, `at`, `szFull`),
  ADD KEY `CATACCTApc0` (`hcatInterest`),
  ADD KEY `CATACCTApc1` (`hcatPrincipal`),
  ADD KEY `CATACCTApc2` (`hcatService`),
  ADD KEY `CATACCTApc3` (`hcatOpenAdj`),
  ADD KEY `CATACCTApc4` (`hcatEndAdj`),
  ADD KEY `CATACCTEmp1` (`hcatEmpMatch`),
  ADD KEY `CATACCTEmp2` (`hcatEmpMatchPost`),
  ADD KEY `CRNCACCT` (`hcrnc`),
  ADD KEY `hacctRel` (`hacctRel`),
  ADD KEY `hfiAcct` (`hfi`),
  ADD UNIQUE KEY `NoDupesOnAddr` (`haddr`),
  ADD UNIQUE KEY `SuperTypeName` (`ast`, `szFull`),
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`),
  ADD UNIQUE KEY `TypeAndName` (`at`, `szFull`);
ALTER TABLE `ADV`
  ADD KEY `ADVT_SUMADV` (`lAdvId`),
  ADD KEY `lCurImpDesc` (`lCurImportance`, `hadv`);
ALTER TABLE `Advisor Important Dates Custom Pool`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `Asset Allocation Custom Pool`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `AUTO`
  ADD UNIQUE KEY `szLic` (`szLic`);
ALTER TABLE `AWD`
  ADD KEY `lHacct` (`lHacct`),
  ADD KEY `PgmDt` (`hpgm`, `dt`),
  ADD KEY `TypeHacct` (`dt`, `awdt`, `lHacct`);
ALTER TABLE `BGT`
  ADD UNIQUE KEY `szFull` (`szFull`);
ALTER TABLE `BGT_BKT`
  ADD UNIQUE KEY `BgtTypeName` (`hbgt`, `bbt`, `szFull`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `BGT_ITM`
  ADD UNIQUE KEY `BgtCatType` (`hbgt`, `hcat`, `hbgtitm`),
  ADD KEY `bgtitmLink` (`hbgtitmLink`),
  ADD KEY `BgtName` (`hbgt`, `szFull`),
  ADD KEY `CATBGT_ITM` (`hcat`),
  ADD KEY `hbgtbkt` (`hbgtbkt`);
ALTER TABLE `BILL`
  ADD KEY `DtHeadIinstItrn` (`dt`, `hbillHead`, `iinst`, `itrn`),
  ADD KEY `HeadDtIinstItrn` (`hbillHead`, `dt`, `iinst`, `itrn`),
  ADD UNIQUE KEY `HeadIinstItrn` (`hbillHead`, `iinst`, `itrn`),
  ADD KEY `HeadItrnIinst` (`hbillHead`, `itrn`, `iinst`),
  ADD KEY `HeadStIinstItrn` (`hbillHead`, `st`, `iinst`, `itrn`),
  ADD KEY `lHtrn` (`lHtrn`),
  ADD KEY `StHeadIinstItrn` (`st`, `hbillHead`, `iinst`, `itrn`);
ALTER TABLE `CAT`
  ADD KEY `AlsHctLevelParent` (`szAls`, `hct`, `nLevel`, `hcatParent`),
  ADD UNIQUE KEY `HctLevelParentAls` (`hct`, `nLevel`, `hcatParent`, `szAls`),
  ADD UNIQUE KEY `HctLevelParentName` (`hct`, `nLevel`, `hcatParent`, `szFull`),
  ADD KEY `NameHctLevelParent` (`szFull`, `hct`, `nLevel`, `hcatParent`),
  ADD UNIQUE KEY `ParentName` (`hcatParent`, `szFull`),
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `CESRC`
  ADD UNIQUE KEY `CeobjtCeoId` (`ceobjt`, `ceoId`);
ALTER TABLE `CLI`
  ADD UNIQUE KEY `szFull` (`szFull`);
ALTER TABLE `CLI_DAT`
  ADD UNIQUE KEY `CliData` (`hcli`, `idData`);
ALTER TABLE `CNTRY`
  ADD KEY `CRNCCNTRY` (`hcrncDef`),
  ADD UNIQUE KEY `szCode` (`szCode`),
  ADD UNIQUE KEY `szFull` (`szFull`);
ALTER TABLE `CRIT`
  ADD KEY `hitm` (`hitm`),
  ADD KEY `VIEWCRIT1` (`hviewSub`),
  ADD KEY `ViewIfc` (`hview`, `ifc`);
ALTER TABLE `CRNC`
  ADD KEY `Lcid` (`lcid`),
  ADD UNIQUE KEY `szFull` (`szName`);
ALTER TABLE `CRNC_EXCHG`
  ADD KEY `CRNCCRNC_EXCHG1` (`hcrncTo`),
  ADD UNIQUE KEY `CRNCSymbol` (`hcrncFrom`, `hcrncTo`, `szSymbol`),
  ADD KEY `exchgid` (`exchgid`),
  ADD KEY `szSymbol` (`szSymbol`);
ALTER TABLE `CT`
  ADD UNIQUE KEY `ctid` (`ctid`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `DHD`
  ADD UNIQUE KEY `hcrncDhd` (`hcrncDef`);
ALTER TABLE `FI`
  ADD UNIQUE KEY `ADDRPOLFI` (`haddrPOL`),
  ADD UNIQUE KEY `haddrFI` (`haddr`),
  ADD UNIQUE KEY `szFull` (`szFull`);
ALTER TABLE `Goal Custom Pool`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `Inventory Custom Pool`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `ITM`
  ADD KEY `CLIITM` (`hcli`),
  ADD KEY `NameIcol` (`szName`, `icolPhys`),
  ADD KEY `PoolIcolTblMatch` (`tblPool`, `icolPhys`, `tblPhys`, `lMatch`),
  ADD KEY `TblIcolMatch` (`tblPhys`, `icolPhys`, `lMatch`),
  ADD UNIQUE KEY `TblName` (`tblPhys`, `szName`),
  ADD KEY `TblSrcDestIcol` (`tblSrc`, `tblPhys`, `icolPhys`);
ALTER TABLE `IVTY`
  ADD KEY `Dt` (`dt`, `ivtyt`, `hivty`),
  ADD KEY `DtIvty` (`dt`, `hivty`),
  ADD KEY `ivtytProd` (`ivtyt`, `hprod`, `dt`, `dQty`),
  ADD UNIQUE KEY `ProdDt` (`hprod`, `dt`, `ivtyt`, `hivty`),
  ADD KEY `ProdDtIvty` (`hprod`, `dt`, `hivty`);
ALTER TABLE `LOT`
  ADD KEY `AcctSecDtTrnBuy` (`hacct`, `hsec`, `dtBuy`, `htrnBuy`),
  ADD KEY `AcctSecDtTrnClose` (`hacct`, `hsec`, `dtClose`, `htrnClose`),
  ADD KEY `AcctSecDtTrnOpen` (`hacct`, `hsec`, `dtOpen`, `htrnOpen`),
  ADD KEY `AcctSecDtTrnSell` (`hacct`, `hsec`, `dtSell`, `htrnSell`),
  ADD KEY `hlotOpenDtClose` (`hlotOpen`, `dtClose`),
  ADD KEY `LOTLINK` (`hlotLink`),
  ADD KEY `LOTOPEN` (`hlotOpen`),
  ADD KEY `SecAcctDtTrnBuy` (`hsec`, `hacct`, `dtBuy`, `htrnBuy`),
  ADD KEY `SecDtTrnBuy` (`hsec`, `dtBuy`, `htrnBuy`),
  ADD KEY `SecDtTrnClose` (`hsec`, `dtClose`, `htrnClose`),
  ADD KEY `SecDtTrnOpen` (`hsec`, `dtOpen`, `htrnOpen`),
  ADD KEY `SecDtTrnSell` (`hsec`, `dtSell`, `htrnSell`),
  ADD KEY `TRN_INVLOT` (`htrnBuy`),
  ADD KEY `TRN_INVLOT1` (`htrnSell`),
  ADD KEY `TrnDtClose` (`htrnClose`, `dtClose`),
  ADD KEY `TrnDtOpen` (`htrnOpen`, `dtOpen`),
  ADD KEY `TrnOpenDtClose` (`htrnOpen`, `dtClose`);
ALTER TABLE `LSTEP`
  ADD KEY `hstepNext` (`hstepNext`),
  ADD KEY `LSTEPLOAN` (`hacctLoan`);
ALTER TABLE `MAIL`
  ADD KEY `ACCTMAIL` (`hacct`),
  ADD UNIQUE KEY `dtSentByFi` (`dtSentByFi`, `hmail`),
  ADD KEY `FIMAIL` (`hfi`);
ALTER TABLE `MCSRC`
  ADD KEY `FDelDtHobj` (`fDeleted`, `dtDeleted`, `mcoId`),
  ADD UNIQUE KEY `FDelObjtDtHobj` (`fDeleted`, `objt`, `dtDeleted`, `mcoId`),
  ADD UNIQUE KEY `ObjtHobj` (`objt`, `lHobj`),
  ADD UNIQUE KEY `ObjtMcoId` (`objt`, `mcoId`);
ALTER TABLE `PAY`
  ADD KEY `dtLast` (`dtLast`, `szFull`),
  ADD UNIQUE KEY `haddrPay` (`haddr`),
  ADD UNIQUE KEY `NoDupOnAddrBill` (`haddrBill`),
  ADD UNIQUE KEY `NoDupOnAddrShip` (`haddrShip`),
  ADD KEY `PAYPAY` (`hpayParent`),
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `PGM`
  ADD KEY `szFull` (`szFull`),
  ADD UNIQUE KEY `TypeAndName` (`pgmt`, `szFull`);
ALTER TABLE `PMT`
  ADD KEY `CustDtTrnInvoice` (`hcust`, `dtInvoice`, `htrnInvoice`),
  ADD KEY `CustDtTrnPmt` (`hcust`, `dtPmt`, `htrnPmt`),
  ADD KEY `TRN_INVOICEPMT` (`htrnInvoice`),
  ADD KEY `TRNPMT` (`htrnPmt`);
ALTER TABLE `PORT_REC`
  ADD KEY `ACCTPORT_REC` (`hacct`),
  ADD KEY `SECPORT_REC` (`hsec`);
ALTER TABLE `Portfolio View Custom Pool`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `POS_STMT`
  ADD KEY `ACCTPOS_STMT` (`hacct`);
ALTER TABLE `PRODUCT`
  ADD KEY `CATPRODUCT` (`hcat`),
  ADD UNIQUE KEY `szFull` (`szFull`);
ALTER TABLE `PROJ`
  ADD KEY `EndEntry` (`dtEnd`, `hproj`),
  ADD KEY `StartEntry` (`dtStart`, `hproj`),
  ADD UNIQUE KEY `szFull` (`szFull`);
ALTER TABLE `PROV_FI`
  ADD UNIQUE KEY `ADDRPROV_FI` (`haddr`),
  ADD KEY `hfiPrvFi` (`hfi`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `PROV_FI_PAY`
  ADD UNIQUE KEY `ADDRPROV_FI_PAY` (`haddr`),
  ADD KEY `FIPROV_FI_PAY` (`hfi`),
  ADD KEY `PAYPROV_FI_PAY` (`hpay`),
  ADD UNIQUE KEY `PfiPay` (`hprovfi`, `hpay`);
ALTER TABLE `Report Custom Pool`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `SAV_GOAL`
  ADD KEY `BGTSAVGOAL` (`hbgt`),
  ADD KEY `dt` (`dt`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `SEC`
  ADD KEY `CNTRYSEC` (`hcntry`),
  ADD KEY `CRNCSEC` (`hcrnc`),
  ADD KEY `SctName` (`sct`, `szFull`),
  ADD KEY `SECSECLINK` (`hsecLink`),
  ADD KEY `szAls` (`szSymbol`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `SIC`
  ADD KEY `CATSIC` (`hcat`);
ALTER TABLE `SOQ`
  ADD KEY `hacctSOQ` (`hacct`),
  ADD UNIQUE KEY `SecAcct` (`hsec`, `hacct`);
ALTER TABLE `SP`
  ADD KEY `ExchgidDateSp` (`exchgid`, `dt`, `hsp`),
  ADD KEY `HsecDateSrcSp` (`hsec`, `dt`, `src`, `hsp`),
  ADD KEY `hssSp` (`hss`);
ALTER TABLE `STMT`
  ADD KEY `ACCTSTMT` (`hacct`),
  ADD UNIQUE KEY `ADDRSTMT` (`haddr`);
ALTER TABLE `SVC`
  ADD UNIQUE KEY `AcctProvfi` (`hacct`, `hprovfi`),
  ADD KEY `hprovfiSvc` (`hprovfi`);
ALTER TABLE `Tax Rate Custom Pool`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `TAXLINE`
  ADD KEY `TxnTxcp` (`txn`, `txcpy`);
ALTER TABLE `TMI`
  ADD UNIQUE KEY `PayStartEnd` (`hpay`, `dtStart`, `dtEnd`, `htmi`),
  ADD UNIQUE KEY `ProdStartEnd` (`hproduct`, `dtStart`, `dtEnd`, `htmi`),
  ADD UNIQUE KEY `StartEnd` (`dtStart`, `dtEnd`, `htmi`);
ALTER TABLE `TRIP`
  ADD KEY `DtEndDtStartHauto` (`dtEnd`, `dtStart`, `hauto`),
  ADD KEY `hautoTrip` (`hauto`);
ALTER TABLE `TRN`
  ADD KEY `AcctCsIdDtTrn` (`hacct`, `cs`, `szId`, `dt`, `htrn`),
  ADD KEY `AcctDtIdTrn` (`hacct`, `dt`, `szId`, `htrn`),
  ADD KEY `AcctIdDtTrn` (`hacct`, `szId`, `dt`, `htrn`),
  ADD KEY `AcctSecDtIdTrn` (`hacct`, `hsec`, `dt`, `szId`, `htrn`),
  ADD KEY `ActDtIdTrn` (`act`, `dt`, `szId`, `htrn`),
  ADD KEY `BILLTRN` (`hbillHead`),
  ADD KEY `CatDtIdTrn` (`hcat`, `dt`, `szId`, `htrn`),
  ADD KEY `Cls1DtIdTrn` (`lHcls1`, `dt`, `szId`, `htrn`),
  ADD KEY `Cls2DtIdTrn` (`lHcls2`, `dt`, `szId`, `htrn`),
  ADD KEY `DtIdTrn` (`dt`, `szId`, `htrn`),
  ADD KEY `hacctLink` (`hacctLink`),
  ADD KEY `IdDtTrn` (`szId`, `dt`, `htrn`),
  ADD KEY `OlttDtTrn` (`oltt`, `dt`, `htrn`),
  ADD KEY `PayDtIdTrn` (`hpay`, `dt`, `szId`, `htrn`),
  ADD KEY `SecDtIdTrn` (`hsec`, `dt`, `szId`, `htrn`),
  ADD KEY `STMTTRN` (`hstmtRel`),
  ADD KEY `TRNTRN` (`htrnSrc`),
  ADD KEY `TxsrcDtTrn` (`lHtxsrc`, `dt`, `htrn`);
ALTER TABLE `TRN_INVOICE`
  ADD KEY `ADDRTRN_INVOICE` (`haddrShip`),
  ADD KEY `ADDRTRN_INVOICE1` (`haddrBill`),
  ADD KEY `hivtyInvoice` (`hivty`),
  ADD KEY `hprojInvoice` (`hproj`),
  ADD KEY `htmiInvoice` (`htmi`),
  ADD KEY `PRODUCTTRN_INVOICE` (`hproduct`);
ALTER TABLE `TRN_SPLIT`
  ADD UNIQUE KEY `SeqInParent` (`htrnParent`, `iSplit`);
ALTER TABLE `TRN_XFER`
  ADD UNIQUE KEY `htrnTrnXferTo` (`htrnLink`);
ALTER TABLE `TXSRC`
  ADD UNIQUE KEY `SrctHsrcTxnTxcpy` (`srct`, `lHsrc`, `txn`, `txcpy`),
  ADD KEY `SrctHsrcTxpdTxn` (`srct`, `lHsrc`, `txpd`, `txn`),
  ADD UNIQUE KEY `TxnCpyTypeSrc` (`txn`, `txcpy`, `srct`, `lHsrc`);
ALTER TABLE `UIE`
  ADD KEY `TypeThemeName` (`elt`, `theme`, `pos`),
  ADD UNIQUE KEY `TypeThemePosSubtype` (`elt`, `theme`, `pos`, `subelt`);
ALTER TABLE `UKSavings`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `UKWiz`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `UKWizAddress`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `UKWizCompanyCar`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `UKWizLoan`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `UKWizMortgage`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `UKWizPenScheme`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `UKWizPension`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `UKWizWillExecutor`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `UKWizWillGift`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `UKWizWillGuardian`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `UKWizWillLovedOne`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `UKWizWillMaker`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `UKWizWillPerson`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `UKWizWillResidue`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `UNOTE`
  ADD UNIQUE KEY `DateSubject` (`dtTarget`, `szSubject`);
ALTER TABLE `VIEW`
  ADD KEY `CLIVIEW` (`hcli`),
  ADD UNIQUE KEY `PooltypeNameCli` (`hitmPool`, `szFull`, `hcli`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `Worksheet Custom Pool`
  ADD KEY `szAls` (`szAls`),
  ADD KEY `szFull` (`szFull`);
ALTER TABLE `XBAG`
  ADD UNIQUE KEY `BagTblHobj` (`bt`, `tbl`, `lHobj`, `lHobjRel`),
  ADD KEY `dtFollowup` (`dtFollowup`),
  ADD KEY `TblHobj` (`tbl`, `lHobj`);
ALTER TABLE `XMAPACCT`
  ADD KEY `ACCTXMAPACCT` (`hacct`),
  ADD KEY `NumBankId` (`szNum`, `szBankId`),
  ADD KEY `SVCXMAPACCT` (`hsvc`);
ALTER TABLE `XMAPSAT`
  ADD UNIQUE KEY `szMCCat` (`szMCCat`);

-- 4. Checks


-- 5. Foreign keys, validated by the server

SET SESSION foreign_key_checks = 1;

SET SESSION sql_mode = @ac_sql_mode, foreign_key_checks = @ac_fk, autocommit = @ac_ac;

