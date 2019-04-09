from pyspark.mllib.recommendation import ALS, Rating
from pyspark.mllib.evaluation import RegressionMetrics
# $example off$
from pyspark import SparkContext
import sys

if __name__ == "__main__":
    sc = SparkContext(appName="Ranking Metrics Example")

    # Several of the methods available in scala are currently missing from pyspark
    # $example on$
    # Read in the ratings data
    lines = sc.textFile(sys.argv[1])

    def parseLine(line):
        fields = line.split("::")
        return Rating(int(fields[0]), int(fields[1]), float(fields[2]) - 2.5)
    ratings = lines.map(lambda r: parseLine(r))

    # Train a model on to predict user-product ratings
    model = ALS.train(ratings, 10, 10, 0.01)

    # Get predicted ratings on all existing user-product pairs
    testData = ratings.map(lambda p: (p.user, p.product))
    predictions = model.predictAll(testData).map(lambda r: ((r.user, r.product), r.rating))

    ratingsTuple = ratings.map(lambda r: ((r.user, r.product), r.rating))
    scoreAndLabels = predictions.join(ratingsTuple).map(lambda tup: tup[1])

    # Instantiate regression metrics to compare predicted and actual ratings
    metrics = RegressionMetrics(scoreAndLabels)

    # Root mean squared error
    print("RMSE = %s" % metrics.rootMeanSquaredError)

    # R-squared
    print("R-squared = %s" % metrics.r2)
