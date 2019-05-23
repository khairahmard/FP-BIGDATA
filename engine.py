import os
import logging
import pandas as pd

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql import types
from pyspark.sql.functions import explode
import pyspark.sql.functions as func

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RecommendationEngine:
    """A product recommendation engine
    """

    def __train_all_model(self):
        """Train the ALS model with the current dataset
        """
           
        #Model 1
        logger.info("Training the ALS model 1")
        self.als = ALS(maxIter=5, regParam=0.01, userCol="UserId", itemCol="ProductId", ratingCol="Rating", coldStartStrategy="drop")
        self.model1 = self.als.fit(self.df0)
        logger.info("ALS model 1 built!")
        
        #Model 2
        logger.info("Training the ALS model 2")
        self.als = ALS(maxIter=5, regParam=0.01, userCol="UserId", itemCol="ProductId", ratingCol="Rating", coldStartStrategy="drop")
        self.model2 = self.als.fit(self.df1)
        logger.info("ALS model 2 built!")
        
        #Model 3
        logger.info("Training the ALS model 3")
        self.als = ALS(maxIter=5, regParam=0.01, userCol="UserId", itemCol="ProductId", ratingCol="Rating", coldStartStrategy="drop")
        self.model3 = self.als.fit(self.df2)
        logger.info("ALS model 3 built!")
        
    def __train_model(self, model):
        """Train the ALS model with the current dataset
        """
        
        logger.info("Training the ALS model...")
        self.als = ALS(maxIter=5, regParam=0.01, userCol="UserId", itemCol="ProductId", ratingCol="Rating", coldStartStrategy="drop")
        if model == 0:
            self.model1 = self.als.fit(self.df0)
        elif model == 1:
            self.model2 = self.als.fit(self.df1)
        elif model == 2:
            self.model3 = self.als.fit(self.df2)
        logger.info("ALS model built!")

    def get_top_ratings(self, model, user_id, products_count):
        """Recommends up to product_count top unrated product to user_id
        """
        
        if model == 0:
            users = self.df0.select(self.als.getUserCol())
            users = users.filter(users.UserId == user_id)
            userSubsetRecs = self.model1.recommendForUserSubset(users, products_count)
            userSubsetRecs = userSubsetRecs.withColumn("recommendations", explode("recommendations"))
            userSubsetRecs = userSubsetRecs.select(func.col('UserId'),
                                                   func.col('recommendations')['ProductId'].alias('ProductId'),
                                                   func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                        drop('recommendations')
            userSubsetRecs = userSubsetRecs.drop('Rating')
            # userSubsetRecs.printSchema()
            userSubsetRecs = userSubsetRecs.toPandas()
            userSubsetRecs = userSubsetRecs.to_json()
            return userSubsetRecs
        elif model == 1:
            users = self.df1.select(self.als.getUserCol())
            users = users.filter(users.UserId == user_id)
            userSubsetRecs = self.model2.recommendForUserSubset(users, products_count)
            userSubsetRecs = userSubsetRecs.withColumn("recommendations", explode("recommendations"))
            userSubsetRecs = userSubsetRecs.select(func.col('UserId'),
                                                   func.col('recommendations')['ProductId'].alias('ProductId'),
                                                   func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                        drop('recommendations')
            userSubsetRecs = userSubsetRecs.drop('Rating')
            # userSubsetRecs.show()
            # userSubsetRecs.printSchema()
            userSubsetRecs = userSubsetRecs.toPandas()
            userSubsetRecs = userSubsetRecs.to_json()
            return userSubsetRecs
        elif model == 2:
            users = self.df2.select(self.als.getUserCol())
            users = users.filter(users.UserId == user_id)
            userSubsetRecs = self.model3.recommendForUserSubset(users, products_count)
            userSubsetRecs = userSubsetRecs.withColumn("recommendations", explode("recommendations"))
            userSubsetRecs = userSubsetRecs.select(func.col('UserId'),
                                                   func.col('recommendations')['ProductId'].alias('ProductId'),
                                                   func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                        drop('recommendations')
            userSubsetRecs = userSubsetRecs.drop('Rating')
            # userSubsetRecs.show()
            # userSubsetRecs.printSchema()
            userSubsetRecs = userSubsetRecs.toPandas()
            userSubsetRecs = userSubsetRecs.to_json()
            return userSubsetRecs

    def get_top_product_recommend(self, model, product_id, user_count):
        """Recommends up to products_count top unrated products to user_id
        """
        
        if model == 0:
            products = self.df0.select(self.als.getItemCol())
            products = products.filter(products.ProductId == product_id)
            productSubsetRecs = self.model1.recommendForItemSubset(products, user_count)
            productSubsetRecs = productSubsetRecs.withColumn("recommendations", explode("recommendations"))
            productSubsetRecs = productSubsetRecs.select(func.col('ProductId'),
                                                     func.col('recommendations')['UserId'].alias('UserId'),
                                                     func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                            drop('recommendations')
            productSubsetRecs = productSubsetRecs.drop('Rating')
            # userSubsetRecs.printSchema()
            productSubsetRecs = productSubsetRecs.toPandas()
            productSubsetRecs = productSubsetRecs.to_json()
            return productSubsetRecs
        elif model == 1:
            products = self.df1.select(self.als.getItemCol())
            products = products.filter(products.ProductId == product_id)
            productSubsetRecs = self.model2.recommendForItemSubset(products, user_count)
            productSubsetRecs = productSubsetRecs.withColumn("recommendations", explode("recommendations"))
            productSubsetRecs = productSubsetRecs.select(func.col('ProductId'),
                                                     func.col('recommendations')['UserId'].alias('UserId'),
                                                     func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                            drop('recommendations')
            productSubsetRecs = productSubsetRecs.drop('Rating')
            # userSubsetRecs.show()
            # userSubsetRecs.printSchema()
            productSubsetRecs = productSubsetRecs.toPandas()
            productSubsetRecs = productSubsetRecs.to_json()
            return productSubsetRecs
        elif model == 2:
            products = self.df2.select(self.als.getItemCol())
            products = products.filter(products.ProductId == product_id)
            productSubsetRecs = self.model3.recommendForItemSubset(products, user_count)
            productSubsetRecs = productSubsetRecs.withColumn("recommendations", explode("recommendations"))
            productSubsetRecs = productSubsetRecs.select(func.col('ProductId'),
                                                     func.col('recommendations')['UserId'].alias('UserId'),
                                                     func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                            drop('recommendations')
            productSubsetRecs = productSubsetRecs.drop('Rating')
            # userSubsetRecs.show()
            # userSubsetRecs.printSchema()
            productSubsetRecs = productSubsetRecs.toPandas()
            productSubsetRecs = productSubsetRecs.to_json()
            return productSubsetRecs

    def get_ratings_for_product_ids(self, model, user_id, product_id):
        """Given a user_id and a list of product_ids, predict ratings for them
        """
        
        if model == 0:
            request = self.spark_session.createDataFrame([(user_id, product_id)], ["UserId", "ProductId"])
            ratings = self.model1.transform(request).collect()
            return ratings
        elif model == 1:
            request = self.spark_session.createDataFrame([(user_id, product_id)], ["UserId", "ProductId"])
            ratings = self.model2.transform(request).collect()
            return ratings
        elif model == 2:
            request = self.spark_session.createDataFrame([(user_id, product_id)], ["UserId", "ProductId"])
            ratings = self.model3.transform(request).collect()
            return ratings

    def __init__(self, spark_session, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """
        logger.info("Starting up the Recommendation Engine: ")
        self.spark_session = spark_session

        # Load Amazon data for later use
        logger.info("Loading Amazon data...")
        file_name1 = 'model_1.txt'
        dataset_file_path1 = os.path.join(dataset_path, file_name1)
        exist = os.path.isfile(dataset_file_path1)
        if exist:
            self.df0 = spark_session.read.csv(dataset_file_path1, header=None, inferSchema=True)
            self.df0 = self.df0.selectExpr("_c0 as UserId", "_c1 as ProductId", "_c2 as Rating")

        file_name2 = 'model_2.txt'
        dataset_file_path2 = os.path.join(dataset_path, file_name2)
        exist = os.path.isfile(dataset_file_path2)
        if exist:
            self.df1 = spark_session.read.csv(dataset_file_path2, header=None, inferSchema=True)
            self.df1 = self.df1.selectExpr("_c0 as UserId", "_c1 as ProductId", "_c2 as Rating")

        file_name3 = 'model_3.txt'
        dataset_file_path3 = os.path.join(dataset_path, file_name3)
        exist = os.path.isfile(dataset_file_path3)
        if exist:
            self.df2 = spark_session.read.csv(dataset_file_path3, header=None, inferSchema=True)
            self.df2 = self.df2.selectExpr("_c0 as UserId", "_c1 as ProductId", "_c2 as Rating")
        
        # Train the model
        self.__train_all_model()
