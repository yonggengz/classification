import os.path
import pandas as pd
import csv
import operator
import csv, sqlite3
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
# nltk.download('stopwords')
# nltk.download('punkt')
import string
import mysql.connector
from mysql.connector import Error


headerList = ['value1', 'id1', 'value2', 'id2']


def create_no_repeate_list(l):
    list = []
    for n in l:
        if n in list:
            list.remove(n)
        list.append(n)
    return list


# def get_data(id1, id2):
#     df = pd.read_csv("gfg2.csv", usecols=headerList, low_memory=False)
#     df = df.loc[df['id1'] == id1 & df['id2'] == id2]
#     df.to_csv(f"{id1}_{id2}.csv", index=False)


def clean(text):
    punctuation_string = string.punctuation
    stopwords_list = stopwords.words('french')
    for i in punctuation_string:
        text = text.replace(i, ' ')
    text = text.strip()
    text = ' '.join(text.split())
    text = text.lower()
    text_tokens = word_tokenize(text)

    temp = text_tokens.copy()
    for text_token in temp:
        if text_token in stopwords_list or text_token == 'la':
            text_tokens.remove(text_token)

    text = ' '.join(text_tokens)
    return text


def clean_dataframe(dataframe, headers):
    list1 = dataframe[headers[0]].values.tolist()
    list2 = dataframe[headers[1]].values.tolist()
    x = 0
    res1 = []
    res2 = []
    for item in list1:
        item = clean(item)
        res1.append(item)
        x += 1
        print(x)
    x=0
    for item in list2:
        item = clean(item)
        res2.append(item)
        x += 1
        print(x)
    d = {headers[0]: res1,
         f"origin_{headers[0]}": list1,
         headers[1]: res2,
         f"origin_{headers[1]}": list2}
    res = pd.DataFrame(data=d)
    return res


def operate_file(input_file):
    df = pd.read_csv(input_file)
    headers = list(df.columns)
    res = clean_dataframe(df, headers)
    return res


def find_element(column_num_src, column_num_dst, word, df):
    for index, info in df.iterrows():
        if info[column_num_src] == word:
            return info[column_num_dst]


def similarity(s1, s2):
    if s2 == s1:
        res = 1
    else:
        count = 0
        for word1 in word_tokenize(s1):
            for word2 in word_tokenize(s2):
                if word1 == word2:
                    count += 1
        res = count / len(s1)
    return res


def func_1to2(df, out_dir):
    headers = list(df.columns)
    column1 = list(dict.fromkeys(df[f"{headers[0]}"]))  # clean duplicates
    print("lenth = ", len(column1))
    title = [headers[0], headers[1], "total",
             f"{headers[2]}_1", f"{headers[3]}_1", f"similarity1_{headers[2]}%", f"probability1_{headers[3]}%",
             f"{headers[2]}_2", f"{headers[3]}_2", f"similarity2_{headers[2]}%", f"probability2_{headers[3]}%",
             f"{headers[2]}_3", f"{headers[3]}_3", f"similarity3_{headers[2]}%", f"probability3_{headers[3]}%",
             f"{headers[2]}_4", f"{headers[3]}_4", f"similarity4_{headers[2]}%", f"probability4_{headers[3]}%",
             f"{headers[2]}_5", f"{headers[3]}_5", f"similarity5_{headers[2]}%", f"probability5_{headers[3]}%"
             ]
    df_res = pd.DataFrame()
    for i in title:
        df_res[i] = ""
    count = 0
    for i in column1:
        df1 = df.loc[df[f"{headers[0]}"] == i]
        num = df1.shape[0]
        df2 = df1[headers[2]]
        res = df2.value_counts()

        if len(res) > 4:
            line = [i, find_element(0, 1, i, df1), num,
                    res.index[0], find_element(2, 3, res.index[0], df1), similarity(i, res.index[0]) * 100, res.array[0] / num * 100,
                    res.index[1], find_element(2, 3, res.index[1], df1), similarity(i, res.index[1]) * 100, res.array[1] / num * 100,
                    res.index[2], find_element(2, 3, res.index[2], df1), similarity(i, res.index[2]) * 100, res.array[2] / num * 100,
                    res.index[3], find_element(2, 3, res.index[3], df1), similarity(i, res.index[3]) * 100, res.array[3] / num * 100,
                    res.index[4], find_element(2, 3, res.index[4], df1), similarity(i, res.index[4]) * 100, res.array[4] / num * 100]
        elif len(res) > 3:
            line = [i, find_element(0, 1, i, df1), num,
                    res.index[0], find_element(2, 3, res.index[0], df1), similarity(i, res.index[0]) * 100, res.array[0] / num * 100,
                    res.index[1], find_element(2, 3, res.index[1], df1), similarity(i, res.index[1]) * 100, res.array[1] / num * 100,
                    res.index[2], find_element(2, 3, res.index[2], df1), similarity(i, res.index[2]) * 100, res.array[2] / num * 100,
                    res.index[3], find_element(2, 3, res.index[3], df1), similarity(i, res.index[3]) * 100, res.array[3] / num * 100, 0, 0, 0, 0]
        elif len(res) > 2:
            line = [i, find_element(0, 1, i, df1), num,
                    res.index[0], find_element(2, 3, res.index[0], df1), similarity(i, res.index[0]) * 100, res.array[0] / num * 100,
                    res.index[1], find_element(2, 3, res.index[1], df1), similarity(i, res.index[1]) * 100, res.array[1] / num * 100,
                    res.index[2], find_element(2, 3, res.index[2], df1), similarity(i, res.index[2]) * 100, res.array[2] / num * 100, 0, 0, 0, 0, 0, 0, 0, 0]
        elif len(res) > 1:
            line = [i, find_element(0, 1, i, df1), num,
                    res.index[0], find_element(2, 3, res.index[0], df1), similarity(i, res.index[0]) * 100, res.array[0] / num * 100,
                    res.index[1], find_element(2, 3, res.index[1], df1), similarity(i, res.index[1]) * 100, res.array[1] / num * 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        elif len(res) > 0:
            line = [i, find_element(0, 1, i, df1), num,
                    res.index[0], find_element(2, 3, res.index[0], df1), similarity(i, res.index[0]) * 100, res.array[0] / num * 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        else:
            line = [i, find_element(0, 1, i, df1), num, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

        df_res.loc[count] = line
        count = count + 1
        print(count)
    df_res.to_csv(out_dir, index=False)


def func_2to1(df, out_dir):
    headers = list(df.columns)
    column1 = list(dict.fromkeys(df[f"{headers[2]}"]))  # clean duplicates
    print("lenth = ", len(column1))
    title = [headers[2], headers[3], "total",
             f"{headers[0]}_1", f"{headers[1]}_1", f"similarity1_{headers[0]}%", f"probability1_{headers[1]}%",
             f"{headers[0]}_2", f"{headers[1]}_2", f"similarity2_{headers[0]}%", f"probability2_{headers[1]}%",
             f"{headers[0]}_3", f"{headers[1]}_3", f"similarity3_{headers[0]}%", f"probability3_{headers[1]}%",
             f"{headers[0]}_4", f"{headers[1]}_4", f"similarity4_{headers[0]}%", f"probability4_{headers[1]}%",
             f"{headers[0]}_5", f"{headers[1]}_5", f"similarity5_{headers[0]}%", f"probability5_{headers[1]}%"
             ]
    df_res = pd.DataFrame()
    for i in title:
        df_res[i] = ""
    count = 0
    for i in column1:
        df1 = df.loc[df[f"{headers[2]}"] == i]
        num = df1.shape[0]
        df2 = df1[headers[0]]
        res = df2.value_counts()

        if len(res) > 4:
            line = [i, find_element(2, 3, i, df1), num,
                    res.index[0], find_element(0, 1, res.index[0], df1), similarity(i, res.index[0]) * 100, res.array[0] / num * 100,
                    res.index[1], find_element(0, 1, res.index[1], df1), similarity(i, res.index[1]) * 100, res.array[1] / num * 100,
                    res.index[2], find_element(0, 1, res.index[2], df1), similarity(i, res.index[2]) * 100, res.array[2] / num * 100,
                    res.index[3], find_element(0, 1, res.index[3], df1), similarity(i, res.index[3]) * 100, res.array[3] / num * 100,
                    res.index[4], find_element(0, 1, res.index[4], df1), similarity(i, res.index[4]) * 100, res.array[4] / num * 100]
        elif len(res) > 3:
            line = [i, find_element(2, 3, i, df1), num,
                    res.index[0], find_element(0, 1, res.index[0], df1), similarity(i, res.index[0]) * 100, res.array[0] / num * 100,
                    res.index[1], find_element(0, 1, res.index[1], df1), similarity(i, res.index[1]) * 100, res.array[1] / num * 100,
                    res.index[2], find_element(0, 1, res.index[2], df1), similarity(i, res.index[2]) * 100, res.array[2] / num * 100,
                    res.index[3], find_element(0, 1, res.index[3], df1), similarity(i, res.index[3]) * 100, res.array[3] / num * 100, 0, 0, 0, 0]
        elif len(res) > 2:
            line = [i, find_element(2, 3, i, df1), num,
                    res.index[0], find_element(0, 1, res.index[0], df1), similarity(i, res.index[0]) * 100, res.array[0] / num * 100,
                    res.index[1], find_element(0, 1, res.index[1], df1), similarity(i, res.index[1]) * 100, res.array[1] / num * 100,
                    res.index[2], find_element(0, 1, res.index[2], df1), similarity(i, res.index[2]) * 100, res.array[2] / num * 100, 0, 0, 0, 0, 0, 0, 0, 0]
        elif len(res) > 1:
            line = [i, find_element(2, 3, i, df1), num,
                    res.index[0], find_element(0, 1, res.index[0], df1), similarity(i, res.index[0]) * 100, res.array[0] / num * 100,
                    res.index[1], find_element(0, 1, res.index[1], df1), similarity(i, res.index[1]) * 100, res.array[1] / num * 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        elif len(res) > 0:
            line = [i, find_element(2, 3, i, df1), num,
                    res.index[0], find_element(0, 1, res.index[0], df1), similarity(i, res.index[0]) * 100, res.array[0] / num * 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        else:
            line = [i, find_element(2, 3, i, df1), num, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

        df_res.loc[count] = line
        count = count + 1
        print(count)
    df_res.to_csv(out_dir, index=False)


def sql_manager():


    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='beetween',
                                             user='root',
                                             password='dstj6d;mvs')
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)

    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


def top(filename):
    name = os.path.splitext(filename)[0]
    col1 = name.split('_')[0]
    col2 = name.split('_')[1]
    df = operate_file(filename)
    func_1to2(df, f"{col1}to{col2}.csv")
    func_2to1(df, f"{col2}to{col1}.csv")


def takeSecond(elem):
    return elem[1]


if __name__ == '__main__':
    # top("135_134.csv")
    list1 = [('276', 1), ('88', 4), ('229', 8), ('372', 15), ('379', 15), ('438', 15), ('439', 15), ('1', 16),
             ('474', 16), ('381', 27), ('385', 32), ('420', 43), ('279', 65), ('216', 67), ('176', 74), ('462', 90),
             ('319', 141), ('74', 177), ('357', 295), ('238', 337), ('412', 394), ('433', 572), ('143', 763),
             ('84', 770), ('388', 908), ('193', 934), ('93', 943), ('159', 1004), ('226', 1051), ('452', 1326),
             ('153', 1408), ('374', 1423), ('278', 1531), ('409', 1859), ('384', 2026), ('249', 2282), ('387', 2413),
             ('218', 3315), ('282', 3568), ('56', 3740), ('217', 3828), ('144', 4005), ('138', 4361), ('161', 4663),
             ('155', 4768), ('154', 4808), ('312', 5073), ('430', 9469), ('150', 9538), ('69', 12108), ('245', 13234),
             ('344', 21113), ('212', 24428), ('473', 48687), ('230', 59515), ('129', 61710), ('406', 96489),
             ('405', 97054), ('317', 97322), ('335', 97332), ('331', 101561), ('147', 143791), ('33', 587049),
             ('277', 620668), ('386', 857533), ('224', 1321833), ('222', 1321999), ('223', 1322071), ('134', 1332360),
             ('135', 1405162)]
    list2 = [135, 134, 223, 222, 224, 386, 277, 33, 147, 331, 335, 317, 405, 406, 129, 230, 473, 212, 344, 245, 69, 150, 430,
     312, 154, 155, 161, 138, 144, 217, 56, 282, 218, 387, 249, 384, 409, 278, 374, 153, 452, 226, 159, 93, 193, 388,
     84, 143, 433, 412, 238, 357, 74, 319, 462, 176, 216, 279, 420, 385, 381, 1, 474, 372, 379, 438, 439, 229, 88, 276]

    # list.sort(key=takeSecond, reverse=True)

    sql_manager()