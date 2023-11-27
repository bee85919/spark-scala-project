import re
import os
import json
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ArrayType
from pyspark.sql.functions import explode, map_keys, col, first, get_json_object, array, to_json, struct, split, regexp_replace, trim

base_path = '/Users/parkjisook/Desktop/yeardream/medistream/js/json/output'


def save_to_csv(df, name):
    path = os.path.join(base_path, name)
    # DataFrame을 하나의 파일로 저장
    # coalesce(1)을 사용하여 모든 데이터를 단일 파티션으로 합침
    df.coalesce(1).write.mode('append').option("encoding", "utf-8").csv(path, header=True)


def nth_json_path(n):
    return f'/Users/parkjisook/Desktop/yeardream/medistream/js/json/naverplace_meta/naverplace_meta{n}.json'


def read_write_txt():
    file_path = '/Users/parkjisook/Desktop/yeardream/medistream/js/json/test.txt'
    with open(file_path, 'r') as file:
        lines = file.readlines()
    n = lines.pop(0).strip()
    with open(file_path, 'w') as file:
        file.writelines(lines)
    return n


spark = SparkSession.builder \
    .appName("medi_test") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()


# get string dataframe!
def get_string_df(df, string_columns, string_df):
    get_string_cols = df.select(string_columns)
    string_row = get_string_cols.filter(~col('name').isNull())
    return string_df.union(string_row)


file_path = '/Users/parkjisook/Desktop/yeardream/medistream/js/json/test.txt'
n = read_write_txt()
data = spark.read.json(nth_json_path(n))

columns = data.columns

hospital_bases = [c for c in columns if "HospitalBase" in c]

schema = StructType([
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

string_df = spark.createDataFrame([], schema)

# hospital_bases_dict를 사용하여 Row 객체 생성 description
hospital_data = []

for hospital_base, base_id in zip(hospital_bases[:50], [base.split(":")[1].strip() for base in hospital_bases[:50]]):
    id_value = data.select(f"HospitalBase:{base_id}.id").filter(
        ~col('id').isNull()).first()
    name_value = data.select(f"HospitalBase:{base_id}.name").filter(
        ~col('name').isNull()).first()
    review_settings = data.select(f"HospitalBase:{base_id}.reviewSettings").filter(
        ~col('reviewSettings').isNull()).first()
    description_value = data.select(f"HospitalBase:{base_id}.description").filter(
        ~col('description').isNull()).first()
    road_value = data.select(f"HospitalBase:{base_id}.road").filter(
        ~col('road').isNull()).first()
    bookingBusinessId_value = data.select(f"HospitalBase:{base_id}.bookingBusinessId").filter(~col('bookingBusinessId').isNull()).first()
    bookingDisplayName_value = data.select(f"HospitalBase:{base_id}.bookingDisplayName").filter(~col('bookingDisplayName').isNull()).first()
    category_value = data.select(f"HospitalBase:{base_id}.category").filter(
        ~col('category').isNull()).first()
    categoryCode_value = data.select(f"HospitalBase:{base_id}.categoryCode").filter(
        ~col('categoryCode').isNull()).first()
    categoryCodeList_value = data.select(f"HospitalBase:{base_id}.categoryCodeList").filter(
        ~col('categoryCodeList').isNull()).first()
    categoryCount_value = data.select(f"HospitalBase:{base_id}.categoryCount").filter(
        ~col('categoryCount').isNull()).first()
    rcode_value = data.select(f"HospitalBase:{base_id}.rcode").filter(
        ~col('rcode').isNull()).first()
    virtualPhone_value = data.select(f"HospitalBase:{base_id}.virtualPhone").filter(
        ~col('virtualPhone').isNull()).first()
    phone_value = data.select(f"HospitalBase:{base_id}.phone").filter(
        ~col('phone').isNull()).first()
    naverBookingUrl_value = data.select(f"HospitalBase:{base_id}.naverBookingUrl").filter(
        ~col('naverBookingUrl').isNull()).first()
    conveniences_value = data.select(f"HospitalBase:{base_id}.conveniences").filter(
        ~col('conveniences').isNull()).first()
    talktalkUrl_value = data.select(f"HospitalBase:{base_id}.talktalkUrl").filter(
        ~col('talktalkUrl').isNull()).first()
    keywords_value = data.select(f"HospitalBase:{base_id}.keywords").filter(
        ~col('keywords').isNull()).first()
    paymentInfo_value = data.select(f"HospitalBase:{base_id}.paymentInfo").filter(
        ~col('paymentInfo').isNull()).first()

    # Check if values are not None before extracting the first element
    id_value = id_value[0] if id_value else None
    name_value = name_value[0] if name_value else None
    keyword_value = review_settings[0]['keyword'] if review_settings else None
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

    road_value = road_value.replace("\n", "").replace("\r", "").replace("*", "").replace(",", " ") if road_value else None
    description_value = description_value.replace("\n", " ").replace("\r", " ").replace("*", "").replace(",", " ") if description_value else None
    keyword_value = keyword_value.replace("&", "").replace(
        "|", "").replace("\\", "") if keyword_value is not None else None

    # Row 객체 생성
    row = Row(
        id=base_id,
        name=name_value,
        keyword=keyword_value,
        description=description_value,
        road=road_value,
        bookingBusinessId=bookingBusinessId_value,
        bookingDisplayName=bookingDisplayName_value,
        category=category_value,
        categoryCode=categoryCode_value,
        categoryCodeList=categoryCodeList_value,
        categoryCount=categoryCount_value,
        rcode=rcode_value,
        virtualPhone=virtualPhone_value,
        phone=phone_value,
        naverBookingUrl=naverBookingUrl_value,
        conveniences=conveniences_value,
        talktalkUrl=talktalkUrl_value,
        keywords=keywords_value,
        paymentInfo=paymentInfo_value
    )
    hospital_data.append(row)

# 데이터와 열 이름을 사용하여 데이터프레임 생성
hospital_df = spark.createDataFrame(hospital_data, schema=schema)

# 중복 제거 후 새로운 데이터프레임 생성
deduplicated_df = hospital_df.dropDuplicates([
    "id",
    "name",
    "keyword",
    "description",
    "road",
    "bookingBusinessId",
    "bookingDisplayName",
    "category",
    "categoryCode",
    "categoryCodeList",
    "categoryCount",
    "rcode",
    "virtualphone",
    "phone",
    "naverBookingUrl",
    "conveniences",
    "talktalkUrl",
    "keywords",
    "paymentInfo"
])
save_to_csv(deduplicated_df, "hospital_data_deduplicated")



