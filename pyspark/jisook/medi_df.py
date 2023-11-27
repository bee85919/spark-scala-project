import re
import os
import json
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, ArrayType
from pyspark.sql.functions import explode, map_keys, col, first, get_json_object, array, to_json, struct,split,regexp_replace,trim,when,size,length

base_path = '/Users/parkjisook/Desktop/yeardream/medistream/js/json/new_output'


def save_to_csv(df, name):
    path = os.path.join(base_path, name)
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
    StructField('road_address', StringType(), True),
    StructField('keywords', StringType(), True),
    StructField('payment_info', StringType(), True),
    StructField('ref', StringType(), True),
    StructField('lon', StringType(), True),
    StructField('lat', StringType(), True) 
])

string_df = spark.createDataFrame([], schema)

# hospital_bases_dict를 사용하여 Row 객체 생성 description
hospital_data = []

for hospital_base, base_id in zip(hospital_bases[:50], [base.split(":")[1].strip() for base in hospital_bases[:50]]):
    id_value = data.select(f"HospitalBase:{base_id}.id").filter(~col('id').isNull()).first()
    name_value = data.select(f"HospitalBase:{base_id}.name").filter(~col('name').isNull()).first()
    review_settings = data.select(f"HospitalBase:{base_id}.reviewSettings").filter(~col('reviewSettings').isNull()).first()
    description_value = data.select(f"HospitalBase:{base_id}.description").filter(~col('description').isNull()).first()
    road_value = data.select(f"HospitalBase:{base_id}.road").filter(~col('road').isNull()).first()
    booking_business_id_value = data.select(f"HospitalBase:{base_id}.bookingBusinessId").filter(~col('bookingBusinessId').isNull()).first()
    booking_display_name_value = data.select(f"HospitalBase:{base_id}.bookingDisplayName").filter(~col('bookingDisplayName').isNull()).first()
    category_value = data.select(f"HospitalBase:{base_id}.category").filter(~col('category').isNull()).first()
    category_code_value = data.select(f"HospitalBase:{base_id}.categoryCode").filter(~col('categoryCode').isNull()).first()
    category_code_list_value = data.select(f"HospitalBase:{base_id}.categoryCodeList").filter(~col('categoryCodeList').isNull()).first()
    category_count_value = data.select(f"HospitalBase:{base_id}.categoryCount").filter(~col('categoryCount').isNull()).first()
    rcode_value = data.select(f"HospitalBase:{base_id}.rcode").filter(~col('rcode').isNull()).first()
    virtual_phone_value = data.select(f"HospitalBase:{base_id}.virtualPhone").filter(~col('virtualPhone').isNull()).first()
    phone_value = data.select(f"HospitalBase:{base_id}.phone").filter(~col('phone').isNull()).first()
    naver_booking_url_value = data.select(f"HospitalBase:{base_id}.naverBookingUrl").filter(~col('naverBookingUrl').isNull()).first()
    conveniences_value = data.select(f"HospitalBase:{base_id}.conveniences").filter(~col('conveniences').isNull()).first()
    talktalk_url_value = data.select(f"HospitalBase:{base_id}.talktalkUrl").filter(~col('talktalkUrl').isNull()).first()
    road_address_value = data.select(f"HospitalBase:{base_id}.roadAddress").filter(~col('roadAddress').isNull()).first()
    keywords_value = data.select(f"HospitalBase:{base_id}.keywords").filter(~col('keywords').isNull()).first()
    payment_info_value = data.select(f"HospitalBase:{base_id}.paymentInfo").filter(~col('paymentInfo').isNull()).first()
    streetPanorama_value = data.select(f"HospitalBase:{base_id}.streetPanorama").filter(~col('streetPanorama').isNull()).first()

    id_value = id_value[0] if id_value else None
    name_value = name_value[0] if name_value else None
    keyword_value = review_settings[0]['keyword'] if review_settings else None
    description_value = description_value[0] if description_value else None
    road_value = road_value[0] if road_value else None
    booking_business_id_value = booking_business_id_value[0] if booking_business_id_value else None
    booking_display_name_value = booking_display_name_value[0] if booking_display_name_value else None
    category_value = category_value[0] if category_value else None
    category_code_value = category_code_value[0] if category_code_value else None
    category_code_list_value = category_code_list_value[0] if category_code_list_value else None
    category_count_value = category_count_value[0] if category_count_value else None
    rcode_value = rcode_value[0] if rcode_value else None
    virtual_phone_value = virtual_phone_value[0] if virtual_phone_value else None
    phone_value = phone_value[0] if phone_value else None
    naver_booking_url_value = naver_booking_url_value[0] if naver_booking_url_value else None
    conveniences_value = conveniences_value[0] if conveniences_value else None
    talktalk_url_value = talktalk_url_value[0] if talktalk_url_value else None
    road_address_value = road_address_value[0] if road_address_value else None
    keywords_value = keywords_value[0] if keywords_value else None
    payment_info_value = payment_info_value[0] if payment_info_value else None
    ref_value = streetPanorama_value[0]['__ref'] if streetPanorama_value else None
    lon_lat_values = None
    lon_lat_values = None
    if ref_value:
        panorama_key = ref_value.split(":")[1]
        panorama_data = data.selectExpr(f"`Panorama:{panorama_key}` as panorama_data").filter(~col('panorama_data').isNull()).first()
        if panorama_data:
            lon_lat_values = {
                'lon': float(panorama_data[0]['lon']),
                'lat': float(panorama_data[0]['lat'])
            }

    road_value = road_value.replace("\n", "").replace("\r", "").replace("*", "").replace(",", " ") if road_value else None
    description_value = description_value.replace("\n", " ").replace("\r", " ").replace("*", "").replace(",", " ") if description_value else None
    keyword_value = keyword_value.replace("\\", "").replace("\"", "") if keyword_value is not None else None

    # Row 객체 생성
    row = Row(
        id=id_value,
        name=name_value,
        keyword=keyword_value,
        description=description_value,
        road=road_value,
        booking_business_id=booking_business_id_value,
        booking_display_name=booking_display_name_value,
        category=category_value,
        category_code=category_code_value,
        category_code_list=category_code_list_value,
        category_count=category_count_value,
        rcode=rcode_value,
        virtual_phone=virtual_phone_value,
        phone=phone_value,
        naver_booking_url=naver_booking_url_value,
        conveniences=conveniences_value,
        talktalk_url=talktalk_url_value,
        road_address=road_address_value,
        keywords=keywords_value,
        payment_info=payment_info_value,
        ref=ref_value,
        lon=lon_lat_values['lon'] if lon_lat_values else None,
        lat=lon_lat_values['lat'] if lon_lat_values else None
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
    "road_address",
    "keywords",
    "payment_info"
])


#is_smart_phone
deduplicated_df = deduplicated_df.withColumn(
    'is_smart_phone',
    when(col('virtual_phone').isNull() | (col('virtual_phone') == ''), False).otherwise(True)
)

#description_length
hospital_df = hospital_df.withColumn('description_length', length('description'))

# payment_info & zero_pay
deduplicated_df = deduplicated_df.withColumn(
    'zero_pay',
    when(
        (col('payment_info').isNotNull()) & (col('payment_info').contains('제로페이')),
        True
    ).otherwise(False)
)

#keywords
deduplicated_df = deduplicated_df.withColumn(
    'keywords_array',
    split(col('keywords'), ',')
)

deduplicated_df = deduplicated_df.withColumn(
    'keywords_1', col('keywords_array')[0]
).withColumn(
    'keywords_2', col('keywords_array')[1]
).withColumn(
    'keywords_3', col('keywords_array')[2]
).withColumn(
    'keywords_4', col('keywords_array')[3]
).withColumn(
    'keywords_5', col('keywords_array')[4]
)

# Replace "[" and "]" in keyword_n columns
for i in range(1, 6):
    col_name = f'keywords_{i}'
    deduplicated_df = deduplicated_df.withColumn(
        col_name,
        when(col(col_name).isNotNull(), regexp_replace(col(col_name), "[\[\]]", ""))
    )
    
deduplicated_df = deduplicated_df.drop('keywords_array')

# deduplicated_df=deduplicated_df.select(
#     "id",
#     "name",
#     "keyword",
#     "description",
#     "description_length",
#     "road",
#     "booking_business_id",
#     "booking_display_name",
#     "category",
#     "category_code",
#     "category_code_list",
#     "category_count",
#     "rcode",
#     "virtual_phone",
#     "is_smart_phone",
#     "phone",
#     "naver_booking_url",
#     "conveniences",
#     "talktalk_url",
#     "road_address",
#     "keywords",
#     "keywords_1",
#     "keywords_2",
#     "keywords_3",
#     "keywords_4",
#     "keywords_5",
#     "payment_info",
#     "zero_pay",
#     "lon",
#     "lat"
# )

save_to_csv(deduplicated_df, "hospital_data_deduplicated_updated")





