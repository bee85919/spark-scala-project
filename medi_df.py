import re
import os
import json
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ArrayType
from pyspark.sql.functions import explode, map_keys, col, first, get_json_object, array, to_json, struct, split, regexp_replace, trim


spark = SparkSession.builder \
    .appName("medi_test") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
    
    
root_path = '/Users/parkjisook/Desktop/yeardream/medistream/js/json'
json_root_path = f'{root_path}/naverplace_meta'
save_root_path = f'{root_path}/output'
text_root_path = f'{root_path}/test.txt'


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


columns = data.columns
hospital_bases = [c for c in columns if "HospitalBase" in c]

df_schema = StructType([
    StructField('id', StringType(), True),
    StructField('name', StringType(), True),
    StructField('keyword', StringType(), True),
    StructField('description', StringType(), True),
    StructField('road', StringType(), True),
    StructField('bookingBusinessId', StringType(), True),
    StructField('bookingDisplayName', StringType(), True),
    StructField('category', StringType(), True),
    StructField('categoryCode', StringType(), True),
    StructField('categoryCodeList', StringType(), True),
    StructField('categoryCount', StringType(), True),
    StructField('rcode', StringType(), True),
    StructField('virtualPhone', StringType(), True),
    StructField('phone', StringType(), True),
    StructField('naverBookingUrl', StringType(), True),
    StructField('conveniences', StringType(), True),
    StructField('talktalkUrl', StringType(), True),
    StructField('keywords', StringType(), True),
    StructField('paymentInfo', StringType(), True)
])
df = spark.createDataFrame([], df_schema)


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
    
def save_to_csv(df, name, n):
    save_path = f'{save_root_path}/{name}'
    df.coalesce(1).write.mode('append').option("encoding", "utf-8").csv(save_path, header=True)


hospital_data = []
for hospital_base, base_id in zip(hospital_bases[:50], [hospital_base.split(":")[1].strip() for hospital_base in hospital_bases[:50]]):
    # Get Values
    id_value = get_value(data, base_id, 'id')
    name_value = get_value(data, base_id, 'name')
    review_settings_value = get_value(data, base_id, 'review_settings')
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

    # Check None
    id_value = id_value[0] if id_value else None
    name_value = name_value[0] if name_value else None
    keyword_value = review_settings_value[0]['keyword'] if review_settings_value else None
    description_value = description_value[0] if description_value else None
    road_value = road_value[0] if road_value else None
    bookingBusinessId_value = bookingBusinessId_value[0] if bookingBusinessId_value else None
    bookingDisplayName_value = bookingDisplayName_value[0] if bookingDisplayName_value else None
    category_value = category_value[0] if category_value else None
    categoryCode_value = categoryCode_value[0] if categoryCode_value else None
    categoryCodeList_value = categoryCodeList_value[0] if categoryCodeList_value else None
    categoryCount_value = categoryCount_value[0] if categoryCount_value else None
    rcode_value = rcode_value[0] if rcode_value else None
    virtualPhone_value = virtualPhone_value[0] if virtualPhone_value else None
    phone_value = phone_value[0] if phone_value else None
    naverBookingUrl_value = naverBookingUrl_value[0] if naverBookingUrl_value else None
    conveniences_value = conveniences_value[0] if conveniences_value else None
    talktalkUrl_value = talktalkUrl_value[0] if talktalkUrl_value else None
    keywords_value = keywords_value[0] if keywords_value else None
    paymentInfo_value = paymentInfo_value[0] if paymentInfo_value else None
    keyword_value = keyword_value if keyword_value is not None else None
    
    # Replace expressions and get values
    road_value = replace_expr_and_get_value(road_value)
    description_value = replace_expr_and_get_value(description_value)
    
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
    
df = spark.createDataFrame(hospital_data, schema=df_schema)

# drop duplications
# select columns
hospiata_data = df.dropDuplicates([
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

save_to_csv(hospiata_data, "hospital_datas")