


import os
import sys


os.environ['SPARK_HOME']="/MSAN_USF/courses_spring/630_AdvMachinelearning/spark/spark-1.6.0-bin-hadoop2.6/"

sys.path.append("/MSAN_USF/courses_spring/630_AdvMachinelearning/spark/spark-1.6.0-bin-hadoop2.6/python")

def use_naive_nayes():
    """
    Running the Naive Bayes from Spark's Mlib library
    """

    #loading the files
    #path = "/Users/abhisheksingh29895/Desktop/courses/CURRENT/Advance_Machine_Learning/HW2/aclImdb/"
    path = "/MSAN_USF/courses_spring/630_AdvMachinelearning/code/data/nb_review_classifier/"
    train_pos = sc.textFile(path + "train/pos/*txt").map(lambda line: line.encode('utf8')).map(lambda line: line.split())
    train_neg = sc.textFile(path + "train/neg/*txt").map(lambda line: line.encode('utf8')).map(lambda line: line.split())
    test_pos = sc.textFile(path + "test/pos/*txt").map(lambda line: line.encode('utf8')).map(lambda line: line.split())
    test_neg = sc.textFile(path + "test/neg/*txt").map(lambda line: line.encode('utf8'))
    #TF-IDF
    tr_pos = HashingTF().transform(train_pos);  tr_pos_idf = IDF().fit(tr_pos)
    tr_neg = HashingTF().transform(train_neg);  tr_neg_idf = IDF().fit(tr_neg)
    te_pos = HashingTF().transform(test_pos);  te_pos_idf = IDF().fit(te_pos)
    te_neg = HashingTF().transform(test_neg);  te_neg_idf = IDF().fit(te_neg)
    #IDF step
    tr_pos_tfidf = tr_pos_idf.transform(tr_pos)  ;  tr_neg_tfidf = tr_neg_idf.transform(tr_neg)
    te_pos_tfidf = te_pos_idf.transform(te_pos)  ;  te_neg_tfidf = te_neg_idf.transform(te_neg)
    #Creating labels
    tr_pos_label = [1] * 54  ;  tr_pos_label = sc.parallelize(tr_pos_label)
    te_pos_label = [1] * 54  ;  te_pos_label = sc.parallelize(te_pos_label)
    tr_neg_label = [0] * 57  ;  tr_neg_label = sc.parallelize(tr_neg_label)
    te_neg_label = [0] * 54  ;  te_neg_label = sc.parallelize(te_neg_label)
    # Combine using zip
    nparts = tr_pos_label.getNumPartitions() + tr_pos_tfidf.getNumPartitions()
    train_pos_file = tr_pos_label.repartition(nparts).zip(tr_pos_tfidf.repartition(nparts)).map(lambda x: LabeledPoint(x[0], x[1]))
    train_neg_file = tr_neg_label.zip(tr_neg_tfidf).map(lambda x: LabeledPoint(x[0], x[1]))
    test_pos_file = te_pos_label.zip(te_pos_tfidf).map(lambda x: LabeledPoint(x[0], x[1]))
    test_neg_file = te_neg_label.zip(te_neg_tfidf).map(lambda x: LabeledPoint(x[0], x[1]))
    #Joining 2 RDDS to form the final training set
    train_file = train_pos_file.union(train_neg_file)
    test_file = test_pos_file.union(test_neg_file)
    # Fitting a Naive bayes model
    model = NaiveBayes.train(train_file)
    # Make prediction and test accuracy
    predictionAndLabel = test_file.map(lambda p: (model.predict(p[1]), p[0]))
    accuracy = 1.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / test.count()
    print ""
    print "Test accuracy is {}".format(round(accuracy,4))


try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.mllib.classification import NaiveBayes
    from pyspark.mllib.feature import HashingTF, IDF
    from pyspark.mllib.linalg import SparseVector, Vectors
    from pyspark.mllib.regression import LabeledPoint

    print ("Successfully imported Spark Modules")

    if __name__ == "__main__":
        master = "local"
        if len(sys.argv) == 2:
            master = sys.argv[1]
        sc = SparkContext(master, "WordCount")
        lines = sc.parallelize(["pandas", "i like pandas"])
        result = lines.flatMap(lambda x: x.split(" ")).countByValue()
        for key, value in result.iteritems():
            print "%s %i" % (key, value)


except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)

