package org.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object CreditFinalSelectionCol {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().
      setMaster("local[*]").
      setAppName("test").
      set("spark.ui.enabled", "false").
      set("spark.app.id", "SparkHiveTests").
      set("spark.driver.host", "localhost")

    //val sparkContext = new SparkContext(sparkConf)


    val spark = SparkSession.builder
      .config(sparkConf).getOrCreate()

    //Create Spark Schema For Response Json Files
    val schemaStr: String = """{"type":"struct","fields":[{"name":"creditScore","type":{"type":"struct","fields":[{"name":"agencyData","type":{"type":"struct","fields":[{"name":"businessPartners","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"businessPartnerNumber","type":"string","nullable":true},{"name":"businessPartnerRole","type":"string","nullable":true},{"name":"keyPositionFlag","type":"integer","nullable":true},{"name":"businessRelationSince","type":"string","nullable":true},{"name":"potentialMatchFlag","type":"integer","nullable":true},{"name":"handlingKey","type":"string","nullable":true},{"name":"handlingKeyDate","type":"string","nullable":true},{"name":"singleBorrowerUnitNumber","type":"string","nullable":true},{"name":"singleBorrowerUnitMainNumber","type":"string","nullable":true},{"name":"defaultStatus","type":"string","nullable":true},{"name":"defaultStatusSinceDate","type":"string","nullable":true},{"name":"worstHandlingKeyBU","type":"string","nullable":true},{"name":"worstHandlingKeyBUDate","type":"string","nullable":true},{"name":"foundationDate","type":"string","nullable":true},{"name":"businessExperienceSince","type":"string","nullable":true},{"name":"legalEntityTypeCode","type":"string","nullable":true},{"name":"startUpCompanyFlag","type":"integer","nullable":true},{"name":"industrySectorCode","type":"string","nullable":true},{"name":"firstName","type":"string","nullable":true},{"name":"lastName","type":"string","nullable":true},{"name":"companyName","type":"string","nullable":true},{"name":"gender","type":"string","nullable":true},{"name":"birthDate","type":"string","nullable":true},{"name":"crefoNumber","type":"string","nullable":true},{"name":"schufaRequestFeature","type":"string","nullable":true},{"name":"addresses","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"countryCode","type":"string","nullable":true},{"name":"streetName","type":"string","nullable":true},{"name":"streetNumber","type":"string","nullable":true},{"name":"postalCode","type":"string","nullable":true},{"name":"addressType","type":"string","nullable":true},{"name":"city","type":"string","nullable":true}]},"containsNull":true},"nullable":true},{"name":"phoneNumbers","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"phoneNumber","type":"string","nullable":true}]},"containsNull":true},"nullable":true},{"name":"schufaDisclosure","type":{"type":"struct","fields":[{"name":"schufaScore","type":"integer","nullable":true},{"name":"schufaScoreAnswer","type":"string","nullable":true},{"name":"schufaScoreBereich","type":"string","nullable":true},{"name":"schufaCriterias","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"schufaCriteriaCounter","type":"integer","nullable":true},{"name":"schufaCriteriaType","type":"string","nullable":true},{"name":"schufaCriteriaCode","type":"string","nullable":true},{"name":"schufaCriteriaAmount","type":"double","nullable":true},{"name":"schufaCriteriaMonthlyInstallmentAmount","type":"double","nullable":true},{"name":"schufaCriteriaNumberOfInstallments","type":"integer","nullable":true},{"name":"schufaCriteriaEndDate","type":"string","nullable":true},{"name":"schufaCriteriaAccountNumber","type":"string","nullable":true},{"name":"schufaCriteriaEventDate","type":"string","nullable":true}]},"containsNull":true},"nullable":true},{"name":"schufaFirstName","type":"string","nullable":true},{"name":"schufaLastName","type":"string","nullable":true},{"name":"schufaStreetName","type":"string","nullable":true},{"name":"schufaPostalCode","type":"string","nullable":true},{"name":"schufaCity","type":"string","nullable":true},{"name":"schufaNumberOfNegativeCriterias","type":"integer","nullable":true},{"name":"schufaNumberOfPositiveCriterias","type":"integer","nullable":true},{"name":"schufaNumberOfCheckingAccounts","type":"integer","nullable":true},{"name":"schufaNumberOfCreditCards","type":"integer","nullable":true},{"name":"schufaReportId","type":"string","nullable":true},{"name":"schufaProductState","type":"string","nullable":true},{"name":"schufaValidAsOfDate","type":"string","nullable":true}]},"nullable":true},{"name":"vcDisclosure","type":{"type":"struct","fields":[{"name":"vcIndex","type":"integer","nullable":true},{"name":"vcNegativeCriteria","type":"integer","nullable":true},{"name":"vcFoundationDate","type":"string","nullable":true},{"name":"vcLegalForm","type":"string","nullable":true},{"name":"vcIndustrySectorCode","type":"string","nullable":true},{"name":"vcAssociates","type":"integer","nullable":true},{"name":"vcParticipations","type":"integer","nullable":true},{"name":"vcParticipationsAndAssociates","type":"integer","nullable":true},{"name":"vcNegativeNotesAssociatesAndParticipations","type":"integer","nullable":true},{"name":"vcBalanceSheetTotal","type":"double","nullable":true},{"name":"vcRegisteredCapital","type":"double","nullable":true},{"name":"vcTurnoverAmount","type":"double","nullable":true},{"name":"vcNumberOfEmployees","type":"integer","nullable":true},{"name":"vcInquiryCounter28Days","type":"integer","nullable":true},{"name":"vcInquiryCounter56Days","type":"integer","nullable":true},{"name":"vcInquiryCounter365Days","type":"integer","nullable":true},{"name":"vcValidAsOfDate","type":"string","nullable":true}]},"nullable":true},{"name":"monthsWithArrears","type":"double","nullable":true},{"name":"arrearsLiabilityRatio","type":"double","nullable":true},{"name":"contractsWithDurationUnder12M","type":"double","nullable":true},{"name":"proposalAndContracts","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"contract","type":{"type":"struct","fields":[{"name":"contractNumber","type":"string","nullable":true},{"name":"ocaNumber","type":"string","nullable":true},{"name":"contractEndDate","type":"string","nullable":true},{"name":"relatedContractNumber","type":"string","nullable":true},{"name":"status","type":"string","nullable":true},{"name":"daimlerEmployeeBusinessFlag","type":"integer","nullable":true},{"name":"disableAutomaticApprovalFlag","type":"integer","nullable":true},{"name":"eadAmount","type":"double","nullable":true},{"name":"productCode","type":"string","nullable":true},{"name":"iban","type":"string","nullable":true},{"name":"leasingContractTransferFlag","type":"integer","nullable":true},{"name":"leasingSpecialPaymentAmount","type":"double","nullable":true},{"name":"monthlyInstallmentNetAmount","type":"double","nullable":true},{"name":"plannedContractDurationInMonths","type":"integer","nullable":true},{"name":"defaultStatus","type":"string","nullable":true},{"name":"arrearsAmount","type":"double","nullable":true},{"name":"approvalStatus","type":"string","nullable":true},{"name":"dealerAddress","type":{"type":"struct","fields":[{"name":"countryCode","type":"string","nullable":true},{"name":"streetName","type":"string","nullable":true},{"name":"streetNumber","type":"string","nullable":true},{"name":"postalCode","type":"string","nullable":true},{"name":"addressType","type":"string","nullable":true},{"name":"city","type":"string","nullable":true}]},"nullable":true}]},"nullable":true},{"name":"vehicle","type":{"type":"struct","fields":[{"name":"purchasePriceNetAmount","type":"double","nullable":true},{"name":"marketValueAmount","type":"double","nullable":true},{"name":"residualValueAmount","type":"double","nullable":true},{"name":"mileageAtContractStart","type":"double","nullable":true},{"name":"typeOfAssetLevel5","type":"string","nullable":true},{"name":"initialRegistrationDate","type":"string","nullable":true},{"name":"typeOfAssetBasicElement","type":"string","nullable":true},{"name":"vehicleStatusCode","type":"string","nullable":true},{"name":"vehicleModelFeatures","type":"string","nullable":true},{"name":"powerUnit","type":"string","nullable":true}]},"nullable":true},{"name":"individualDisclosure","type":{"type":"struct","fields":[{"name":"maritalStatusCode","type":"string","nullable":true},{"name":"occupationTypeCode","type":"string","nullable":true},{"name":"employedSince","type":"string","nullable":true},{"name":"individualDisclosureDate","type":"string","nullable":true},{"name":"netIncomeAmount","type":"double","nullable":true},{"name":"totalExpensesAmount","type":"double","nullable":true},{"name":"numberOfCohabitants","type":"integer","nullable":true},{"name":"numberOfUnderagedChildren","type":"integer","nullable":true}]},"nullable":true},{"name":"corporationDisclosures","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"financialStatementDate","type":"string","nullable":true},{"name":"financialStatementType","type":"string","nullable":true},{"name":"businessAssignmentFlag","type":"integer","nullable":true},{"name":"tnwr","type":"double","nullable":true},{"name":"ebitda","type":"double","nullable":true},{"name":"pcdc","type":"double","nullable":true},{"name":"sustainableDCSSurplus","type":"double","nullable":true}]},"containsNull":true},"nullable":true},{"name":"scoringSteeringInformation","type":{"type":"struct","fields":[{"name":"scoringRequestDate","type":"string","nullable":true},{"name":"scoringType","type":"string","nullable":true},{"name":"portfolioSegment","type":"string","nullable":true},{"name":"processCode","type":"string","nullable":true},{"name":"proposalScoringDate","type":"string","nullable":true}]},"nullable":true},{"name":"switchOffPolicyRulesFlag","type":"integer","nullable":true},{"name":"historicalCreditDecision","type":{"type":"struct","fields":[{"name":"creditDecision","type":"string","nullable":true},{"name":"creditDecisionDate","type":"string","nullable":true},{"name":"leasingSpecialPaymentAmount","type":"double","nullable":true}]},"nullable":true}]},"containsNull":true},"nullable":true}]},"containsNull":true},"nullable":true}]},"nullable":true},{"name":"documents","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true},{"name":"id","type":"string","nullable":true},{"name":"scoringResponse","type":{"type":"struct","fields":[{"name":"result","type":{"type":"struct","fields":[{"name":"scoreResult","type":{"type":"struct","fields":[{"name":"masterPD","type":"double","nullable":true},{"name":"modelPD","type":"double","nullable":true},{"name":"modelPDAfterDefault","type":"double","nullable":true},{"name":"scoreClass","type":"string","nullable":true},{"name":"scoreCardVersion","type":"string","nullable":true},{"name":"scoreRequestDate","type":"string","nullable":true},{"name":"scoringDate","type":"string","nullable":true},{"name":"scoringValue","type":"double","nullable":true},{"name":"subScores","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"characteristicPoints","type":"integer","nullable":true},{"name":"characteristicName","type":"string","nullable":true}]},"containsNull":true},"nullable":true},{"name":"hashKey","type":"string","nullable":true}]},"nullable":true},{"name":"lgdResult","type":{"type":"struct","fields":[{"name":"lgdStatus","type":"string","nullable":true},{"name":"lgdModelSegment","type":"string","nullable":true},{"name":"lgdBasePercentage","type":"double","nullable":true},{"name":"lgdEADAmountCurrent","type":"double","nullable":true},{"name":"lgdEADAmountEntrance","type":"double","nullable":true},{"name":"lgdEADCorrection","type":"double","nullable":true},{"name":"lgdModelErrorPremium","type":"double","nullable":true},{"name":"lgdDownturnPremium","type":"double","nullable":true},{"name":"lgdDataQualityPremium","type":"double","nullable":true},{"name":"lgdIndirectCostPremium","type":"double","nullable":true},{"name":"lgdPercentage","type":"double","nullable":true},{"name":"beelPercentage","type":"double","nullable":true},{"name":"lgdModelVersion","type":"string","nullable":true},{"name":"lgdRequestDate","type":"string","nullable":true},{"name":"lgdDate","type":"string","nullable":true}]},"nullable":true},{"name":"elResult","type":{"type":"struct","fields":[{"name":"elAmount","type":"double","nullable":true},{"name":"elPercentage","type":"double","nullable":true},{"name":"elMatrixSegment","type":"integer","nullable":true}]},"nullable":true},{"name":"approvalResult","type":{"type":"struct","fields":[{"name":"policyRules","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"ruleNumber","type":"string","nullable":true},{"name":"ruleLabel","type":"string","nullable":true},{"name":"ruleText","type":"string","nullable":true}]},"containsNull":true},"nullable":true},{"name":"conditions","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"conditionNumber","type":"string","nullable":true},{"name":"conditionText","type":"string","nullable":true},{"name":"conditionAmount","type":"double","nullable":true}]},"containsNull":true},"nullable":true},{"name":"decisionCode","type":"string","nullable":true},{"name":"processingClass","type":"string","nullable":true},{"name":"manualProcessingText","type":"string","nullable":true},{"name":"mailboxCode","type":"string","nullable":true},{"name":"policyRuleCheckFlag","type":"integer","nullable":true},{"name":"highRiskIndicatorFlag","type":"integer","nullable":true},{"name":"additionalDecisionInformation","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"description","type":"string","nullable":true}]},"containsNull":true},"nullable":true}]},"nullable":true},{"name":"calculatedVariables","type":{"type":"struct","fields":[{"name":"stringVariables","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"name","type":"string","nullable":true},{"name":"value","type":"string","nullable":true}]},"containsNull":true},"nullable":true},{"name":"integerVariables","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"name","type":"string","nullable":true},{"name":"value","type":"integer","nullable":true}]},"containsNull":true},"nullable":true},{"name":"decimalVariables","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"name","type":"string","nullable":true},{"name":"value","type":"double","nullable":true}]},"containsNull":true},"nullable":true},{"name":"acidInput","type":{"type":"struct","fields":[{"name":"acidVar1","type":"string","nullable":true}]},"nullable":true},{"name":"acidOutput","type":{"type":"struct","fields":[{"name":"probApprovedWithoutConditions","type":"double","nullable":true},{"name":"probApprovedWithDownPayment","type":"double","nullable":true},{"name":"probApprovedWithGuarantor","type":"double","nullable":true},{"name":"probDeclined","type":"double","nullable":true},{"name":"modelPrediction","type":"string","nullable":true},{"name":"probModelPrediction","type":"double","nullable":true},{"name":"requestedDownPayment","type":"double","nullable":true},{"name":"versionH2OPackage","type":"string","nullable":true},{"name":"versionMojo","type":"string","nullable":true},{"name":"bullShitFlag","type":"integer","nullable":true}]},"nullable":true}]},"nullable":true},{"name":"version","type":"string","nullable":true},{"name":"riskProfile","type":{"type":"struct","fields":[{"name":"refELAbsolut","type":"double","nullable":true},{"name":"refELPercentage","type":"double","nullable":true},{"name":"bprTrigger","type":"string","nullable":true},{"name":"deactivatedBPRs","type":"string","nullable":true},{"name":"refCriteriaRiskRelevance","type":"integer","nullable":true}]},"nullable":true}]},"nullable":true},{"name":"scoringDate","type":"string","nullable":true},{"name":"scoringId","type":"string","nullable":true}]},"nullable":true}]},"nullable":true}]}"""

    val responseType: DataType = DataType.fromJson(schemaStr)

    val responseStructType = responseType.asInstanceOf[StructType]


    val validto = "9999-12-31T23:59:59.999"

    //Read (or) Fetch required Columns from Response Json files


    val rawDF = spark.read.format("org.apache.spark.sql.execution.datasources.json.JsonFileFormat").schema(responseStructType).option("multiLine", "true")
      .load("C:/Users/KPUVVAD/Desktop/Project/Project_Modules/MBLD/creditPlatform/Credit/Sample data/response_data.json")
    rawDF.show(20,false
    )
    val dataTypes=rawDF.dtypes
    println(s"dataTypes:",dataTypes)
    val schema=rawDF.printSchema()
    println(s"schema: ",schema)





    //C:\Users\KPUVVAD\Desktop\Project\Project_Modules\MBLD\creditPlatform\Credit\Sample data


    val explodedDF = rawDF.select(explode(col("creditScore")("agencyData")("businessPartners")).as("businessPartners"),
      to_timestamp(date_format(regexp_replace(split(col("creditScore")("scoringResponse")("result")("scoreResult")("scoringDate"), raw"\[")(0), raw"\+", raw"\-"),
        "yyyy-MM-dd'T'HH:mm:ss.SSSZ")).as("scoringDate"),
      col("creditScore")("scoringResponse")("scoringId").as("scoringId"),
      col("creditScore")("scoringResponse")("result")("scoreResult")("masterPD").as("PD"),
      col("creditScore")("scoringResponse")("result")("scoreResult")("scoreCardVersion").as("Scorecard"),
      col("creditScore")("scoringResponse")("result")("lgdResult")("lgdPercentage").as("LGD"),
      col("creditScore")("scoringResponse")("result")("scoreResult")("scoreClass").as("scoreclass"),


      col("creditScore")("scoringResponse")("result")("lgdResult")("lgdStatus").as("lgdStatus"),
      col("creditScore")("scoringResponse")("result")("lgdResult")("lgdModelSegment").as("lgdModelSegment"),
      col("creditScore")("scoringResponse")("result")("lgdResult")("lgdBasePercentage").as("lgdBasePercentage"),
      col("creditScore")("scoringResponse")("result")("lgdResult")("lgdEADAmountCurrent").as("lgdEADAmountCurrent"),
      col("creditScore")("scoringResponse")("result")("lgdResult")("lgdEADAmountEntrance").as("lgdEADAmountEntrance"),
        col("creditScore")("scoringResponse")("result")("lgdResult")("lgdEADCorrection").as("lgdEADCorrection"),

      col("creditScore")("scoringResponse")("result")("elResult")("elAmount").as("elAmount"),
      col("creditScore")("scoringResponse")("result")("elResult")("elPercentage").as("elPercentage")









    )

    val responseDF = explodedDF.select(col("businessPartners")("businessPartnerNumber").as("customerId"), col("scoringDate"), col("scoringId"), col("PD"), col("Scorecard"), col("LGD"), col("scoreclass"),col("lgdStatus"),col("lgdModelSegment"),col("lgdBasePercentage"), col("lgdEADAmountCurrent"), col("lgdEADAmountEntrance"), col("lgdEADCorrection"), col("elAmount"), col("elPercentage"))
      .withColumn("creationts", col("scoringDate"))
      .withColumn("validfrom", col("scoringDate"))
      .withColumn("validto", to_timestamp(lit(validto)))

    //responseDF.foreach()
    !(responseDF.rdd.isEmpty())
    responseDF.persist()
    responseDF.show(2,false)
    val list = responseDF.collect()
    for (e <- list) {
      println(s"dataframe each record: ",e)
    }

    //responseDF.foreach(row=>row.get(0))
  }
}



      /*output data

        [5000000435,
          null,
          0050d584-f107-459b-b945-a00e8cd1d4fc
    ,
    15.01,
    ScorecardVersion1,
    50.1111111111,
    1
    ,null,null,
    9999-12-31 23:59:59.999]




"lgdStatus"  NOT_DEFAULTED
"lgdModelSegment"  DummyLGDSegment
"lgdBasePercentage" 30.1111111111
"lgdEADAmountCurrent" 100000.01
"lgdEADAmountEntrance"  150000.02
"lgdEADCorrection" 2.43
"elAmount"  100.1111111111
"elPercentage"  10.1111111111



[5000000435,null,0050d584-f107-459b-b945-a00e8cd1d4fc,15.01,ScorecardVersion1,50.1111111111,1,NOT_DEFAULTED,DummyLGDSegment,30.1111111111,100000.01,150000.02,2.43,100.1111111111,10.1111111111,null,null,9999-12-31 23:59:59.999]


*/




