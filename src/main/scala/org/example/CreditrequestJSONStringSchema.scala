package org.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object CreditrequestJSONStringSchema {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().
      setMaster("local[*]").
      setAppName("test").
      set("spark.ui.enabled", "false").
      set("spark.app.id", "SparkHiveTests").
      set("spark.driver.host", "localhost")

    val spark = SparkSession.builder
      .config(sparkConf).getOrCreate()


    //Create Spark Schema For request Json Files
    val requestSchemaStr ="""{"type":"struct","fields":[{"name":"requests","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"requestNumber","type":"string","nullable":true},{"name":"borrowerUnits","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"borrowerUnitRole","type":"string","nullable":true},{"name":"borrowerUnitExposure","type":{"type":"struct","fields":[{"name":"pipelineEADAmountBU","type":"double","nullable":true},{"name":"approvedAndActivatedEADAmountBU","type":"double","nullable":true}]},"nullable":true},{"name":"businessPartners","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"businessPartnerNumber","type":"string","nullable":true},{"name":"businessPartnerRole","type":"string","nullable":true},{"name":"keyPositionFlag","type":"integer","nullable":true},{"name":"businessRelationSince","type":"string","nullable":true},{"name":"potentialMatchFlag","type":"integer","nullable":true},{"name":"handlingKeys","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"handlingKey","type":"string","nullable":true},{"name":"handlingKeyDate","type":"string","nullable":true}]},"containsNull":true},"nullable":true},{"name":"singleBorrowerUnitNumber","type":"string","nullable":true},{"name":"singleBorrowerUnitMainNumber","type":"string","nullable":true},{"name":"foundationDate","type":"string","nullable":true},{"name":"businessExperienceSince","type":"string","nullable":true},{"name":"legalEntityTypeCode","type":"string","nullable":true},{"name":"startUpCompanyFlag","type":"integer","nullable":true},{"name":"industrySectorCode","type":"string","nullable":true},{"name":"firstName","type":"string","nullable":true},{"name":"lastName","type":"string","nullable":true},{"name":"companyName","type":"string","nullable":true},{"name":"gender","type":"string","nullable":true},{"name":"birthDate","type":"string","nullable":true},{"name":"crefoNumber","type":"string","nullable":true},{"name":"schufaRequestFeature","type":"string","nullable":true},{"name":"addresses","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"countryCode","type":"string","nullable":true},{"name":"streetName","type":"string","nullable":true},{"name":"streetNumber","type":"string","nullable":true},{"name":"postalCode","type":"string","nullable":true},{"name":"addressType","type":"string","nullable":true},{"name":"city","type":"string","nullable":true}]},"containsNull":true},"nullable":true},{"name":"phoneNumbers","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"phoneNumber","type":"string","nullable":true}]},"containsNull":true},"nullable":true},{"name":"schufaDisclosure","type":{"type":"struct","fields":[{"name":"schufaScore","type":"integer","nullable":true},{"name":"schufaScoreAnswer","type":"string","nullable":true},{"name":"schufaScoreBereich","type":"string","nullable":true},{"name":"schufaCriterias","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"schufaCriteriaCounter","type":"integer","nullable":true},{"name":"schufaCriteriaType","type":"string","nullable":true},{"name":"schufaCriteriaCode","type":"string","nullable":true},{"name":"schufaCriteriaAmount","type":"double","nullable":true},{"name":"schufaCriteriaMonthlyInstallmentAmount","type":"double","nullable":true},{"name":"schufaCriteriaNumberOfInstallments","type":"integer","nullable":true},{"name":"schufaCriteriaEndDate","type":"string","nullable":true},{"name":"schufaCriteriaAccountNumber","type":"string","nullable":true},{"name":"schufaCriteriaEventDate","type":"string","nullable":true}]},"containsNull":true},"nullable":true},{"name":"schufaFirstName","type":"string","nullable":true},{"name":"schufaLastName","type":"string","nullable":true},{"name":"schufaStreetName","type":"string","nullable":true},{"name":"schufaPostalCode","type":"string","nullable":true},{"name":"schufaCity","type":"string","nullable":true},{"name":"schufaNumberOfNegativeCriterias","type":"integer","nullable":true},{"name":"schufaNumberOfPositiveCriterias","type":"integer","nullable":true},{"name":"schufaNumberOfCheckingAccounts","type":"integer","nullable":true},{"name":"schufaNumberOfCreditCards","type":"integer","nullable":true},{"name":"schufaProductState","type":"string","nullable":true},{"name":"schufaValidAsOfDate","type":"string","nullable":true}]},"nullable":true},{"name":"vcDisclosure","type":{"type":"struct","fields":[{"name":"vcIndex","type":"integer","nullable":true},{"name":"vcNegativeCriteria","type":"integer","nullable":true},{"name":"vcFoundationDate","type":"string","nullable":true},{"name":"vcLegalForm","type":"string","nullable":true},{"name":"vcIndustrySectorCode","type":"string","nullable":true},{"name":"vcAssociates","type":"integer","nullable":true},{"name":"vcParticipations","type":"integer","nullable":true},{"name":"vcParticipationsAndAssociates","type":"integer","nullable":true},{"name":"vcNegativeNotesAssociatesAndParticipations","type":"integer","nullable":true},{"name":"vcBalanceSheetTotal","type":"double","nullable":true},{"name":"vcRegisteredCapital","type":"double","nullable":true},{"name":"vcTurnoverAmount","type":"double","nullable":true},{"name":"vcNumberOfEmployees","type":"integer","nullable":true},{"name":"vcInquiryCounter28Days","type":"integer","nullable":true},{"name":"vcInquiryCounter56Days","type":"integer","nullable":true},{"name":"vcInquiryCounter365Days","type":"integer","nullable":true},{"name":"vcValidAsOfDate","type":"string","nullable":true}]},"nullable":true},{"name":"monthsWithArrears","type":"double","nullable":true},{"name":"arrearsLiabilityRatio","type":"double","nullable":true},{"name":"contractsWithDurationUnder12M","type":"double","nullable":true},{"name":"proposalAndContracts","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"contract","type":{"type":"struct","fields":[{"name":"contractNumber","type":"string","nullable":true},{"name":"ocaNumber","type":"string","nullable":true},{"name":"creationDate","type":"string","nullable":true},{"name":"contractEndDate","type":"string","nullable":true},{"name":"relatedContractNumber","type":"string","nullable":true},{"name":"status","type":"string","nullable":true},{"name":"daimlerEmployeeBusinessFlag","type":"integer","nullable":true},{"name":"disableAutomaticApprovalFlag","type":"integer","nullable":true},{"name":"eadAmount","type":"double","nullable":true},{"name":"productCode","type":"string","nullable":true},{"name":"iban","type":"string","nullable":true},{"name":"leasingContractTransferFlag","type":"integer","nullable":true},{"name":"leasingSpecialPaymentAmount","type":"double","nullable":true},{"name":"monthlyInstallmentNetAmount","type":"double","nullable":true},{"name":"plannedContractDurationInMonths","type":"integer","nullable":true},{"name":"defaultStatus","type":"string","nullable":true},{"name":"arrearsAmount","type":"double","nullable":true},{"name":"approvalStatus","type":"string","nullable":true},{"name":"branchNumber","type":"string","nullable":true},{"name":"dealerAddress","type":{"type":"struct","fields":[{"name":"countryCode","type":"string","nullable":true},{"name":"streetName","type":"string","nullable":true},{"name":"streetNumber","type":"string","nullable":true},{"name":"postalCode","type":"string","nullable":true},{"name":"addressType","type":"string","nullable":true},{"name":"city","type":"string","nullable":true}]},"nullable":true}]},"nullable":true},{"name":"vehicle","type":{"type":"struct","fields":[{"name":"purchasePriceNetAmount","type":"double","nullable":true},{"name":"marketValueAmount","type":"double","nullable":true},{"name":"residualValueAmount","type":"double","nullable":true},{"name":"mileageAtContractStart","type":"double","nullable":true},{"name":"typeOfAssetLevel5","type":"string","nullable":true},{"name":"initialRegistrationDate","type":"string","nullable":true},{"name":"typeOfAssetBasicElement","type":"string","nullable":true},{"name":"vehicleStatusCode","type":"string","nullable":true},{"name":"vehicleModelFeatures","type":"string","nullable":true},{"name":"powerUnit","type":"string","nullable":true}]},"nullable":true},{"name":"individualDisclosure","type":{"type":"struct","fields":[{"name":"maritalStatusCode","type":"string","nullable":true},{"name":"occupationTypeCode","type":"string","nullable":true},{"name":"employedSince","type":"string","nullable":true},{"name":"individualDisclosureDate","type":"string","nullable":true},{"name":"netIncomeAmount","type":"double","nullable":true},{"name":"totalExpensesAmount","type":"double","nullable":true},{"name":"numberOfCohabitants","type":"integer","nullable":true},{"name":"numberOfUnderagedChildren","type":"integer","nullable":true}]},"nullable":true},{"name":"corporationDisclosures","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"financialStatementDate","type":"string","nullable":true},{"name":"financialStatementType","type":"string","nullable":true},{"name":"businessAssignmentFlag","type":"integer","nullable":true},{"name":"tnwr","type":"double","nullable":true},{"name":"ebitda","type":"double","nullable":true},{"name":"pcdc","type":"double","nullable":true},{"name":"sustainableDCSSurplus","type":"double","nullable":true}]},"containsNull":true},"nullable":true},{"name":"scoringSteeringInformation","type":{"type":"struct","fields":[{"name":"scoringRequestDate","type":"string","nullable":true},{"name":"scoringType","type":"string","nullable":true},{"name":"portfolioSegment","type":"string","nullable":true},{"name":"processCode","type":"string","nullable":true},{"name":"proposalScoringDate","type":"string","nullable":true}]},"nullable":true},{"name":"switchOffPolicyRulesFlag","type":"integer","nullable":true},{"name":"historicalCreditDecision","type":{"type":"struct","fields":[{"name":"creditDecision","type":"string","nullable":true},{"name":"creditDecisionDate","type":"string","nullable":true},{"name":"leasingSpecialPaymentAmount","type":"double","nullable":true}]},"nullable":true}]},"containsNull":true},"nullable":true}]},"containsNull":true},"nullable":true}]},"containsNull":true},"nullable":true}]},"containsNull":true},"nullable":true},{"name":"bulkRequestNumber","type":"string","nullable":true},{"name":"identifiers","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"name","type":"string","nullable":true},{"name":"value","type":"string","nullable":true}]},"containsNull":true},"nullable":true},{"name":"applicationId","type":"string","nullable":true}]}"""


    val requestSchemaStructType: StructType = DataType.fromJson(requestSchemaStr).asInstanceOf[StructType]
    val df= spark.read.format("org.apache.spark.sql.execution.datasources.json.JsonFileFormat").schema(requestSchemaStructType).option("multiLine", "true").load("C:/Users/KPUVVAD/Desktop/Project/Project_Modules/MBLD/creditPlatform/newCreditPlatformJSON/RequestExample_newSchema.json")

   df.coalesce(1).write.mode(SaveMode.Overwrite).format("org.apache.spark.sql.execution.datasources.json.JsonFileFormat").option("ignoreNullFields", "false").save("C:/Users/KPUVVAD/Desktop/Project/Project_Modules/MBLD/creditPlatform/newCreditPlatformJSON/writerequestschema")




  }

}