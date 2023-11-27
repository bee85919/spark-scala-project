### import libraries
import re
import os
import json
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ArrayType
from pyspark.sql.functions import explode, map_keys, col, first, get_json_object, array, to_json, struct, split, regexp_replace, trim


### create spark session
spark = SparkSession.builder \
    .appName("medi_test") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()


### set paths
root_path = '/Users/parkjisook/Desktop/yeardream/medistream/js/json'
json_root_path = f'{root_path}/naverplace_meta'
save_root_path = f'{root_path}/output'
text_root_path = f'{root_path}/test.txt'


### read data
def read_text():
    with open(text_root_path, 'r') as t: 
        l = t.readlines()        
    n = l.pop(0).strip()    
    with open(text_root_path, 'w') as t: 
        t.writelines(l)
    return n

def read_json(n):
    json_path = f'{json_root_path}/naverplace_meta_{n}.json'
    data = spark.read.json(json_path)
    return data

n = read_text()
data = read_json(n)


### columns
columns = data.columns
hospital_bases = [c for c in columns if "HospitalBase" in c]


### create dataframe schema with camelcase
df_schema = StructType([
    StructField('id', StringType(), True),
    StructField('name', StringType(), True),
    StructField('keyword', StringType(), True),
    StructField('description', StringType(), True),
    StructField('road', StringType(), True),
    StructField('booking_business_id', StringType(), True),
    StructField('booking_display_name', StringType(), True),
    StructField('category', StringType(), True),
    StructField('category_code', StringType(), True),
    StructField('category_code_list', StringType(), True),
    StructField('category_count', StringType(), True),
    StructField('rcode', StringType(), True),
    StructField('virtual_phone', StringType(), True),
    StructField('phone', StringType(), True),
    StructField('naver_booking_url', StringType(), True),
    StructField('conveniences', StringType(), True),
    StructField('talktalk_url', StringType(), True),
    StructField('keywords', StringType(), True),
    StructField('payment_info', StringType(), True)
])
df = spark.createDataFrame([], df_schema)


### functions
def get_value(data, base_id, key):
    column_key = f'HospitalBase:{base_id}.{key}'
    column = data.select(column_key)
    row = column.filter(~col(key).isNull())
    value = row.first()
    return value

def replace_expr_and_get_value(value):
    if value:
        value = value \
            .replace("\n", "") \
            .replace("\r", "") \
            .replace(",", " ") \
            .replace("*", "")
        return value
    else:
        return None
    
def check_none(value):
    if value:
        return value[0]
    else:
        return None
    
def save_to_csv(df, name, n):
    save_path = f'{save_root_path}/{name}'
    df.coalesce(1).write.mode('append').option("encoding", "utf-8").csv(save_path, header=True)    


### create rows
hospital_data = []
for hospital_base, base_id in zip(hospital_bases[:50], [hospital_base.split(":")[1].strip() for hospital_base in hospital_bases[:50]]):
    # get values    
    id_value = get_value(data, base_id, 'id')
    name_value = get_value(data, base_id, 'name')
    review_keywords_value = get_value(data, base_id, 'reviewSettings')
    description_value = get_value(data, base_id, 'description')
    road_value = get_value(data, base_id, 'road')
    bookingBusinessId_value = get_value(data, base_id, 'bookingBusinessId')
    bookingDisplayName_value = get_value(data, base_id, 'bookingDisplayName')
    category_value = get_value(data, base_id, 'category')
    categoryCode_value = get_value(data, base_id, 'categoryCode')
    categoryCodeList_value = get_value(data, base_id, 'categoryCodeList')
    categoryCount_value = get_value(data, base_id, 'categoryCount')
    rcode_value = get_value(data, base_id, 'rcode')
    virtualPhone_value = get_value(data, base_id, 'virtualPhone')
    phone_value = get_value(data, base_id, 'phone')
    naverBookingUrl_value = get_value(data, base_id, 'naverBookingUrl')
    conveniences_value = get_value(data, base_id, 'conveniences')
    talktalkUrl_value = get_value(data, base_id, 'talktalkUrl')
    keywords_value = get_value(data, base_id, 'keywords')
    paymentInfo_value = get_value(data, base_id, 'paymentInfo')
    print(f"got HospitalBase:{base_id}'s values")

    # check none
    id_value = check_none(id_value)
    name_value = check_none(name_value)
    keyword_value = review_keywords_value[0]['keyword'] if review_keywords_value else None
    description_value = check_none(description_value)
    road_value = check_none(road_value)
    bookingBusinessId_value = check_none(bookingBusinessId_value)
    bookingDisplayName_value = check_none(bookingDisplayName_value)
    category_value = check_none(category_value)
    categoryCode_value = check_none(categoryCode_value)
    categoryCodeList_value = check_none(categoryCodeList_value)
    categoryCount_value = check_none(categoryCount_value)
    rcode_value = check_none(rcode_value)
    virtualPhone_value = check_none(virtualPhone_value)
    phone_value = check_none(phone_value)
    naverBookingUrl_value = check_none(naverBookingUrl_value)
    conveniences_value = check_none(conveniences_value)
    talktalkUrl_value = check_none(talktalkUrl_value)
    keywords_value = check_none(keywords_value)
    paymentInfo_value = check_none(paymentInfo_value)
    keyword_value = check_none(keyword_value)
    print(f"checked HospitalBase:{base_id}'s values")
    
    # replace expressions
    # get values
    road_value = replace_expr_and_get_value(road_value)
    description_value = replace_expr_and_get_value(description_value)
    print(f"replaced HospitalBase:{base_id}'s expressions")
    
    # create rows
    rows = Row(
        id=base_id,
        name=name_value,
        keyword=keyword_value,
        description=description_value,
        road=road_value,
        booking_business_id=bookingBusinessId_value,
        booking_display_name=bookingDisplayName_value,
        category=category_value,
        category_code=categoryCode_value,
        category_code_list=categoryCodeList_value,
        category_count=categoryCount_value,
        rcode=rcode_value,
        virtual_phone=virtualPhone_value,
        phone=phone_value,
        naver_booking_url=naverBookingUrl_value,
        conveniences=conveniences_value,
        talktalk_url=talktalkUrl_value,
        keywords=keywords_value,
        payment_info=paymentInfo_value
    )
    hospital_data.append(rows)
    print(f"appended HospitalBase:{base_id}'s rows\n")

# create dataframe from list
df = spark.createDataFrame(hospital_data, schema=df_schema)

# deduplicate
# select columns
hospiatal_df = df.dropDuplicates([
    "id",
    "name",
    "keyword",
    "description",
    "road",
    "booking_business_id",
    "booking_display_name",
    "category",
    "category_code",
    "category_code_list",
    "category_count",
    "rcode",
    "virtual_phone",
    "phone",
    "naver_booking_url",
    "conveniences",
    "talktalk_url",
    "keywords",
    "payment_info"
])

# save csv from data
save_to_csv(hospiatal_df, "hospital_df")