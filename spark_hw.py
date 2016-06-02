
import os
import sys
import math
import re
import glob
import argparse

stopWords = ['a', 'able', 'about', 'across', 'after', 'all', 'almost', 'also',
             'am', 'among', 'an', 'and', 'any', 'are', 'as', 'at', 'be',
             'because', 'been', 'but', 'by', 'can', 'cannot', 'could', 'dear',
             'did', 'do', 'does', 'either', 'else', 'ever', 'every', 'for',
             'from', 'get', 'got', 'had', 'has', 'have', 'he', 'her', 'hers',
             'him', 'his', 'how', 'however', 'i', 'if', 'in', 'into', 'is',
             'it', 'its', 'just', 'least', 'let', 'like', 'likely', 'may',
             'me', 'might', 'most', 'must', 'my', 'neither', 'no', 'nor',
             'not', 'of', 'off', 'often', 'on', 'only', 'or', 'other', 'our',
             'own', 'rather', 'said', 'say', 'says', 'she', 'should', 'since',
             'so', 'some', 'than', 'that', 'the', 'their', 'them', 'then',
             'there', 'these', 'they', 'this', 'tis', 'to', 'too', 'twas', 'us',
             've', 'wants', 'was', 'we', 'were', 'what', 'when', 'where', 'which',
             'while', 'who', 'whom', 'why', 'will', 'with', 'would', 'yet',
             'you', 'your', 's', 't', 're']

def parsedoc(doc):
    doc = re.sub('[^A-Za-z]+', ' ', doc)
    return doc

def parse_argument():
    """
    Code for parsing arguments
    """
    parser = argparse.ArgumentParser(description='Parsing a file.')
    parser.add_argument('--books', nargs=1, required=True)
    args = vars(parser.parse_args())
    return args

def removeStopWords(word):
    return_word = ' '
    if word not in stopWords and len(word) > 2:
        return_word= word

    return return_word

def calculate_pos_prob(ip, denom_p):
    test_count = ip[1][0]
    tr_p = ip[1][1]
    prob_pos = test_count * (math.log((1+tr_p)*1.0/float(denom_p)))
    return prob_pos

def calculate_neg_prob(ip, denom_n):
    test_count = ip[1][0]
    tr_n = ip[1][1]
    prob_neg = test_count * (math.log((1+tr_n)*1.0/float(denom_n)))
    return prob_neg

def get_prob_label1(ip, pos4, neg4, denom_p, denom_n ):
    prob_pos = 0.0
    prob_neg = 0.0

    for word in ip:
        wc = ip[word]
        tr_p = pos4[word]
        tr_n = neg4[word]
        prob_pos += wc * (math.log((1+tr_p)*1.0/float(denom_p)))
        prob_neg += wc * (math.log((1+tr_n)*1.0/float(denom_n)))

    if prob_pos > prob_neg:
        label_doc = 1.0
    elif prob_pos < prob_neg:
        label_doc = 0.0
    else:
        label_doc =  1.0

    return label_doc

def filelist(pathspec):
    files_list = list()
    for name in glob.glob(pathspec):
        if (os.stat(name).st_size != 0):
            files_list.append(name)
    return files_list



def isHeader(line):
    return "User-ID" in line

def func_question1_2(folderpath):

    path_books = folderpath #"/MSAN_USF/courses_spring/630_AdvMachinelearning/code/data/BX-CSV-Dump/"
    book_ratings = sc.textFile(path_books+"BX-Book-Ratings.csv")
    lines = book_ratings.filter(lambda p: "User-ID" not in p)
    split_lines = lines.map(lambda x: (x.split(";")[0], x.split(";")[1], x.split(";")[2]))
    cnt = split_lines.count()
    print "number of ratings is %d"%(cnt)
    explicit = split_lines.map(lambda x: x[2]).filter(lambda x: x != u'"0"')
    explicit_count = explicit.count()
    print "number of explicit ratings is %d"%(explicit_count)

    dict_ratings = split_lines.map(lambda x: x[2]).filter(lambda x: x != u'"0"').countByValue()
    print "distribution of ratings", dict_ratings

def func_question3(folderpath):
    path_books = folderpath #"/MSAN_USF/courses_spring/630_AdvMachinelearning/code/data/BX-CSV-Dump/"

    book_ratings = sc.textFile(path_books+"BX-Book-Ratings.csv")
    lines = book_ratings.filter(lambda p: "User-ID" not in p)
    pairs = lines.map(lambda x:(x.split(";")[0],int(x.split(";")[2].lstrip('"').rstrip('"'))))

    book_users = sc.textFile(path_books + "BX-Users.csv")
    users = book_users.filter(lambda p: "User-ID" not in p)
    user_id_city = users.map(lambda x: (x.split(";")[0], x.split(";")[1]))

    jjj = pairs.join(user_id_city)
    kkk = jjj.map(lambda x: (x[1][1], (int(x[1][0]), 1)))
    ppp = kkk.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))

    qqq = ppp.mapValues(lambda x:round(x[0]*1.0/x[1],3))
    dict_rrr =qqq.collectAsMap()
    qqq.take(5)

def func_question4(folderpath):
    path_books = folderpath #"/MSAN_USF/courses_spring/630_AdvMachinelearning/code/data/BX-CSV-Dump/"
    book_users = sc.textFile(path_books + "BX-Users.csv")
    users = book_users.filter(lambda p: "User-ID" not in p)

    book_ratings = sc.textFile(path_books+"BX-Book-Ratings.csv")
    lines = book_ratings.filter(lambda p: "User-ID" not in p)

    user_id_city = users.map(lambda x: (x.split(";")[0], x.split(";")[1]))
    user_id_rating = lines.map(lambda x:(x.split(";")[0],1))
    city_rating_count = user_id_rating.join(user_id_city)
    city_dummy = city_rating_count.map(lambda x: (x[1][1], x[1][0]))
    city_count = city_dummy.reduceByKey(lambda x,y: x+y)

    top_5_cities = city_count.takeOrdered(1, key = lambda x: -x[1])

def func_question5(folderpath):
    path_books = folderpath #"/MSAN_USF/courses_spring/630_AdvMachinelearning/code/data/BX-CSV-Dump/"

    book_authors = sc.textFile(path_books+"BX-Books.csv")
    no_header_book_author = book_authors.filter(lambda p: "ISBN" not in p)
    book_author_map = no_header_book_author.map(lambda x: ((x.split(";")[0]), (x.split(";")[2])))

    book_ratings = sc.textFile(path_books+"BX-Book-Ratings.csv")
    lines = book_ratings.filter(lambda p: "User-ID" not in p)
    book_ratings = lines.map(lambda x: (x.split(";")[1],1))
    books = book_ratings.join(book_author_map)

    books_dummy = books.map(lambda x: (x[1][1], x[1][0]))
    author_distribution =  books_dummy.reduceByKey(lambda x,y:x+y).sortByKey().collectAsMap()

    author_distribution

    path_books = folderpath #"/MSAN_USF/courses_spring/630_AdvMachinelearning/code/data/BX-CSV-Dump/"
    # book_users = sc.textFile(path_books + "BX-Users.csv")
    # users = book_users.filter(lambda p: "User-ID" not in p)
    # users = users.map(lambda x: x.split(";")[0])

    book_ratings = sc.textFile(path_books+"BX-Book-Ratings.csv")
    lines = book_ratings.filter(lambda p: "User-ID" not in p)
    book_ratings = lines.map(lambda x: (x.split(";")[0],1))
    users_distribution =  book_ratings.reduceByKey(lambda x,y:x+y).sortByKey()
    users_distribution.take(5)


def naive_sctarch():
    #sc = SparkContext(master, "WordCount")
    AWS_ACCESS_KEY_ID = "AKIAJ4TL25ORBKDDWH2A"
    AWS_SECRET_ACCESS_KEY = "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"

    sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)

    tr_folder = "s3n://usf-ml2/hwspark/train/"
    tr_neg_path = tr_folder+ "neg/*.txt"
    tr_pos_path = tr_folder+ "pos/*.txt"
    neg_files = sc.textFile(tr_neg_path)
    pos_files = sc.textFile(tr_pos_path)

    neg = neg_files.map(lambda x: x.replace(',',' ').replace('.', ' ').replace('-',' ').lower())
    neg1= neg.flatMap(lambda x:x.split())
    neg2 = neg1.map(lambda x: (x,1))
    neg3 = neg2.reduceByKey(lambda x,y: x+y)
    negcount = neg2.count()

    pos = pos_files.map(lambda x: x.replace(',',' ').replace('.', ' ').replace('-',' ').lower())
    pos1 = pos.flatMap(lambda x:x.split())
    pos2 = pos1.map(lambda x: (x,1))
    pos3 = pos2.reduceByKey(lambda x,y: x+y)

    poscount = pos2.count()

    all = neg3.union(pos3)
    unique_count = all.reduceByKey(lambda x,y: x+y).count()
    denom_p = poscount+unique_count+1
    denom_n = negcount+unique_count+1

    te_folder = "s3n://usf-ml2/hw_spark/test/"
    test_Npath = te_folder+"neg/*.txt"
    test_Ppath = te_folder+ "pos/*.txt"

    files_p = filelist(test_Ppath)
    files_n = filelist(test_Npath)
    pos_file_count = len(files_p)
    neg_file_count = len(files_n)
    pr_pos = (pos_file_count)*1.0/(pos_file_count + neg_file_count)*1.0
    pr_neg = (neg_file_count)*1.0/(pos_file_count + neg_file_count)*1.0
    dict_files = {}
    test_n = sc.textFile(test_Npath).collect()
    i = 0
    for doc_n in test_n:
        new_doc = parsedoc(doc_n)
        doc_n_rdd = sc.parallelize(new_doc.split()).map(lambda x: removeStopWords(x))
        t_neg = doc_n_rdd.map(lambda x: x.replace(',',' ').replace('.', ' ').replace('-',' ').lower())
        t_neg1 = t_neg.flatMap(lambda x:x.split())
        t_neg2 = t_neg1.map(lambda x: (x,1))
        #print file_n
        t_neg3 = t_neg2.reduceByKey(lambda x,y: x+y)

        test_N_only = t_neg3.subtractByKey(neg3)
        both_N = t_neg3.join(neg3)
        test_N_only = test_N_only.map(lambda x: (x[0], (x[1], 0)))
        un_N_test = test_N_only.union(both_N)

        un_N_test1 = un_N_test.map(lambda x: calculate_neg_prob(x, denom_n))
        neg_sum = math.log(pr_neg) + un_N_test1.sum()

        np_only = t_neg3.subtractByKey(pos3)
        np_both = t_neg3.join(pos3)
        np_only = np_only.map(lambda x: (x[0], (x[1], 0)))
        un_np_test = np_only.union(np_both)

        un_np_test1 = un_np_test.map(lambda x: calculate_pos_prob(x, denom_p))
        neg_pos_sum = math.log(pr_pos) + un_np_test1.sum()

        if neg_sum > neg_pos_sum:
            dict_files[i] = (0, 0)
        else:
            dict_files[i] = (1, 0)
        i = i + 1

    test_p = sc.textFile(test_Ppath).collect()

    for doc_p in test_p:
        new_doc = parsedoc(doc_p)
        doc_p_rdd = sc.parallelize(new_doc.split()).map(lambda x: removeStopWords(x))
        t_pos = doc_p_rdd.map(lambda x: x.replace(',',' ').replace('.', ' ').replace('-',' ').lower())
        t_pos1 = t_pos.flatMap(lambda x:x.split())
        t_pos2 = t_pos1.map(lambda x: (x,1))
        t_pos3 = t_pos2.reduceByKey(lambda x,y: x+y)

        test_only = t_pos3.subtractByKey(pos3)
        both = t_pos3.join(pos3)
        test_only = test_only.map(lambda x: (x[0], (x[1], 0)))
        un_test = test_only.union(both)

        un_test1 = un_test.map(lambda x: calculate_pos_prob(x, denom_p))
        pos_sum = un_test1.sum()

        pn_only = t_pos3.subtractByKey(pos3)
        pn_both = t_pos3.join(pos3)
        pn_only = pn_only.map(lambda x: (x[0], (x[1], 0)))
        un_pn_test = pn_only.union(pn_both)

        un_pn_test1 = un_pn_test.map(lambda x: calculate_neg_prob(x, denom_n))
        pos_neg_sum = math.log(pr_neg) + un_pn_test1.sum()

        if pos_sum > pos_neg_sum:
            dict_files[i] = (1, 1)
        else:
            dict_files[i] = (0, 1)
        i = i + 1

    sum_matching = 0.0
    total_items = len(dict_files.keys())
    for key in dict_files.keys():
        tup = dict_files[key]
        if tup[0] == tup[1]:
            sum_matching +=1
    accur = (sum_matching)*1.0/(total_items *1.0)
    print accur

def naivebayes_mllib():
    AWS_ACCESS_KEY_ID = "AKIAJ4TL25ORBKDDWH2A"
    AWS_SECRET_ACCESS_KEY = "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"

    sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)

    tr_folder = "s3n://usf-ml2/hwspark/train/"
    tr_neg_path = tr_folder+ "neg/*.txt"
    neg_files = sc.textFile(tr_neg_path)
    neg = neg_files.map(lambda x: parsedoc(x))
    neg = neg.map(lambda x: x.replace(',',' ').replace('.', ' ').replace('-',' ').lower())
    neg1= neg.flatMap(lambda x:x.split())
    neg1 = neg1.map(lambda x: removeStopWords(x))
    tf = HashingTF().transform(neg1.map(lambda x: x, preservesPartitioning=True))
    neg_tr = tf.map(lambda x: LabeledPoint(0.0, x))

    tr_pos_path = tr_folder+ "pos/*.txt"
    pos_files = sc.textFile(tr_pos_path)
    pos = pos_files.map(lambda x: x.replace(',',' ').replace('.', ' ').replace('-',' ').lower())
    pos = pos.map(lambda x: parsedoc(x))
    pos1= pos.flatMap(lambda x:x.split())
    pos1 = pos1.map(lambda x: removeStopWords(x))
    tf_pos = HashingTF().transform(pos1.map(lambda x: x, preservesPartitioning=True))
    pos_tr = tf_pos.map(lambda x: LabeledPoint(1.0, x))

    training = neg_tr.union(pos_tr)
    model = NaiveBayes.train(training)
    te_folder = "s3n://usf-ml2/hw_spark/test/"
    test_Npath = te_folder+"neg/*.txt"
    test_Ppath = te_folder+ "pos/*.txt"
    test = sc.textFile(test_Npath)
    test_p = sc.textFile(test_Ppath)

    test = test.map(lambda x: parsedoc(x))
    test2= test.flatMap(lambda x:x.split())
    test1 = test2.map(lambda x: x.replace(',',' ').replace('.', ' ').replace('-',' ').lower())
    test2 = test1.map(lambda x: removeStopWords(x))
    tf1 = HashingTF().transform(test2.map(lambda x: x, preservesPartitioning=True))

    test5 = tf1.map(lambda x: LabeledPoint(0.0, x))

    test_p = test_p.map(lambda x: parsedoc(x))
    test_p1 = test_p.map(lambda x: x.replace(',',' ').replace('.', ' ').replace('-',' ').lower())
    test_p2= test_p1.flatMap(lambda x:x.split())
    test_p2 = test_p2.map(lambda x: removeStopWords(x))
    tf_p1 = HashingTF().transform(test_p2.map(lambda x: x, preservesPartitioning=True))

    test_p5 = tf_p1.map(lambda x: LabeledPoint(1.0, x))
    testpn = test5.union(test_p5)
    predictionAndLabel = testpn.map(lambda p: (model.predict(p.features), p.label))
    accuracy = predictionAndLabel.filter(lambda (x, v): x == v).count()*1.0 /float(test2.count()+test_p2.count())
    print "Accuracy is {}".format(round(accuracy,5))


try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.mllib.regression import LabeledPoint
    from pyspark.mllib.feature import HashingTF, IDF
    from pyspark.mllib.classification import LogisticRegressionWithSGD
    from pyspark.mllib.classification import NaiveBayes


    print ("Successfully imported Spark Modules")

    if __name__ == "__main__":
        master = "local"
        argument_book = parse_argument()
        file_path_book = argument_book['book'][0]
        #func_question1_2(file_path_book)
        # func_question3(file_path_book)
        # func_question4(file_path_book)
        # func_question5(file_path_book)
        # naive_sctarch()
        #naivebayes_mllib(

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)



