package com.microsoft.cdm.test

import java.io.File
import java.nio.file.Paths

import com.microsoft.cdm.utils.{CDMModel, Constants}
import org.apache.spark.sql.types._
import org.scalatest.{FunSuite, Matchers}

import scala.io.Source

class ModelTests extends FunSuite with Matchers {

  val modelsDir: String = Paths.get(sys.env("SPARK_CDM_REPO_ROOT"), "data", "models").toString

  test("parse entities") {
    val models = Map(
      "wwimodel.json" -> Seq("Sales BuyingGroups", "Sales CustomerCategories", "Sales Customers", "Sales OrderLines",
        "Sales Orders", "Warehouse Colors", "Warehouse PackageTypes", "Warehouse StockItems")
    )

    models foreach{ case (file: String, trueEntities: Seq[String]) =>
      val model = readModel(Paths.get(modelsDir, file).toString)
      val entities = model.listEntities()
      entities should contain theSameElementsAs trueEntities
    }
  }

  test("parse entity schema") {
    val models = Map(
      "wwimodel.json" -> Map(
        "Sales BuyingGroups" -> StructType(
          Array(StructField("BuyingGroupID", LongType),
            StructField("BuyingGroupName", StringType),
            StructField("LastEditedBy", LongType),
            StructField("ValidFrom", DateType),
            StructField("ValidTo", DateType)
          )
        ),
        "Sales CustomerCategories" -> StructType(
          Array(StructField("CustomerCategoryID", LongType),
            StructField("CustomerCategoryName", StringType),
            StructField("LastEditedBy", LongType),
            StructField("ValidFrom", DateType),
            StructField("ValidTo", DateType)
          )
        ),
        "Sales OrderLines" -> StructType(
          Array(StructField("OrderLineID", LongType),
            StructField("OrderID", LongType),
            StructField("StockItemID", LongType),
            StructField("Description", StringType),
            StructField("PackageTypeID", LongType),
            StructField("Quantity", LongType),
            StructField("UnitPrice", DecimalType(Constants.DECIMAL_PRECISION, 0)),
            StructField("TaxRate", DecimalType(Constants.DECIMAL_PRECISION, 0)),
            StructField("PickedQuantity", LongType),
            StructField("PickingCompletedWhen", DateType),
            StructField("LastEditedBy", LongType),
            StructField("LastEditedWhen", DateType)
          )
        )
      )
    )

    models foreach{ case (file: String, schemas: Map[String, StructType]) =>
      val model = readModel(Paths.get(modelsDir, file).toString)
      schemas foreach { case (entity: String, trueSchema: StructType) =>
        assertEqual(model.schema(entity), trueSchema)
      }
    }

  }

  test("parse entity partitions") {

    val models: Map[String, Map[String, Seq[String]]] = Map(
      "wwimodel.json"  -> Map(
        "Sales BuyingGroups" -> Seq("https://afakeaccount.dfs.core.windows.net/powerbi/WideWorldImporters-1/WideWorldImporters-Orders/Sales%20BuyingGroups/Sales%20BuyingGroups.csv.snapshots/Sales%20BuyingGroups.csv@snapshot=2018-11-20T01:30:19.9031631Z"),
        "Sales CustomerCategories" -> Seq("https://afakeaccount.dfs.core.windows.net/powerbi/WideWorldImporters-1/WideWorldImporters-Orders/Sales%20CustomerCategories/Sales%20CustomerCategories.csv.snapshots/Sales%20CustomerCategories.csv@snapshot=2018-11-20T01:30:20.7451554Z"),
        "Sales Customers" -> Seq("https://afakeaccount.dfs.core.windows.net/powerbi/WideWorldImporters-1/WideWorldImporters-Orders/Sales%20Customers/Sales%20Customers.csv.snapshots/Sales%20Customers.csv@snapshot=2018-11-20T01:30:20.6101666Z"),
        "Sales OrderLines" -> Seq("https://afakeaccount.dfs.core.windows.net/powerbi/WideWorldImporters-1/WideWorldImporters-Orders/Sales%20OrderLines/Sales%20OrderLines.csv.snapshots/Sales%20OrderLines.csv@snapshot=2018-11-20T01:30:24.2051863Z"),
        "Sales Orders" -> Seq("https://afakeaccount.dfs.core.windows.net/powerbi/WideWorldImporters-1/WideWorldImporters-Orders/Sales%20Orders/Sales%20Orders.csv.snapshots/Sales%20Orders.csv@snapshot=2018-11-20T01:30:21.7761960Z"),
        "Warehouse Colors" -> Seq("https://afakeaccount.dfs.core.windows.net/powerbi/WideWorldImporters-1/WideWorldImporters-Orders/Warehouse%20Colors/Warehouse%20Colors.csv.snapshots/Warehouse%20Colors.csv@snapshot=2018-11-20T01:30:20.6891622Z"),
        "Warehouse PackageTypes" -> Seq("https://afakeaccount.dfs.core.windows.net/powerbi/WideWorldImporters-1/WideWorldImporters-Orders/Warehouse%20PackageTypes/Warehouse%20PackageTypes.csv.snapshots/Warehouse%20PackageTypes.csv@snapshot=2018-11-20T01:30:19.8261637Z"),
        "Warehouse StockItems" -> Seq("https://afakeaccount.dfs.core.windows.net/powerbi/WideWorldImporters-1/WideWorldImporters-Orders/Warehouse%20StockItems/Warehouse%20StockItems.csv.snapshots/Warehouse%20StockItems.csv@snapshot=2018-11-20T01:30:20.1191652Z")
      )
    )

    models foreach{ case (file: String, entityPartitions: Map[String, Seq[String]]) =>
      val model = readModel(Paths.get(modelsDir, file).toString)
      entityPartitions foreach { case (entity: String, partitions: Seq[String]) =>
        model.partitionLocations(entity).toSeq should contain theSameElementsAs partitions
      }
    }

  }

  def readModel(file: String): CDMModel = {
    new CDMModel(Source.fromFile(new File(file)).mkString)
  }

  def assertEqual(schema1: StructType, schema2: StructType): Unit = {
    schema1.fields should contain theSameElementsAs schema2.fields
  }

}
