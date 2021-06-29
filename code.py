import ast
import sys
import linecache
import datetime
import time
import json
import boto3
import os
#from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import types
# from pyspark import SparkConf
from pyspark.sql.utils import AnalysisException
# from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField, StringType, StructType 
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql import functions as func
from pyspark.sql.types import *
#from awsglue.utils import getResolvedOptions
from pyspark.sql.window import Window

SC = SparkContext()
SPARK = SparkSession.builder.appName("SimpleApp").getOrCreate()
SQLCONTEXT = SQLContext(SPARK)

def str_rsplit(value, delimeter, limit=None):
    """This is a PySpark UDF which is used to split a string from the right.

    Args:
        value ([String/Column]): This is used to specify the column on which the tranformation
                                 will be performed
        delimeter ([String/Column]): This column is used to specify the delimeter on which the
                                     the string would be split.
        limit ([type], optional): [This column is used to specify the number of splits to perform]
                                  Defaults to None.

    Raises:
        Exception: [Any exception occured during splitting is raised]

    Returns:
        [String/Column ]: The resultant column after the split is performed is returned.
    """
    try:
        if limit:
            return value.rsplit(delimeter, limit)
        return value.rsplit(delimeter)
    except Exception as excep_msg:
        raise excep_msg

def str_split(value, delimeter, limit=None):
    """This is a PySpark UDF which is used to split a string from the right.

    Args:
        value ([String/Column]): This is used to specify the column on which the tranformation
                                 will be performed
        delimeter ([String/Column]): This column is used to specify the delimeter on which the
                                     the string would be split.
        limit ([type], optional): [This column is used to specify the number of splits to perform]
                                  Defaults to None.

    Raises:
        Exception: [Any exception occured during splitting is raised]

    Returns:
        [String/Column ]: The resultant column after the split is performed is returned.
    """
    try:
        if limit:
            return value.split(delimeter, limit)
        return value.split(delimeter)
    except Exception as excep_msg:
        raise excep_msg

SPLIT_UDF = func.udf(str_split, types.ArrayType(types.StringType()))
RSPLIT_UDF = func.udf(str_rsplit, types.ArrayType(types.StringType()))

def archive_files(bucket_name, s3_obj_dict):
    """
    Function to archive raw files in S3. Move the files to processed folder,
    and delete them in raw folder.

    Parameters:
        bucket_name(string): name of S3 bucket.
        s3_obj_dict(dict)  : list of S3 files to be archived.

    """
    try:
        s3res = boto3.resource('s3')
        s3_client = boto3.client('s3')
        for key, value in s3_obj_dict.items():
            old_file_name = value
            new_file_name = value.replace('incoming_data', 'processed_data')
            copy_source = {
                'Bucket': bucket_name,
                'Key': old_file_name
            }
            s3res.meta.client.copy(copy_source, bucket_name, new_file_name)
            s3_client.delete_object(Bucket=bucket_name, Key=old_file_name)
    except ClientError as err:
        print('Failed in archive files :', err)

def get_exception(config_object, debug=True):
    """function to identify the exception that has occurred.
       Parameters:
       debug (boolean): debug, when set to True, prints out the error
       Returns:
       None
    """
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    err_obj = {
        'error_code': str(exc_type),
        'error_message': str(exc_obj)
    }
    config_object["error_type"] = str(exc_type)
    config_object["error_message"] = str(exc_obj)
    if debug:
        err_obj['error_at'] = 'EXCEPTION IN ({},LINE {} "{}")'\
            .format(filename, lineno, line.strip())
    print('Error at:', err_obj['error_at'])
    # call error payload
    flag = error_payload_builder(config_object)
    if flag:
        sys.exit(1)

def build_success_payload(config_object):
    """
    This function is used to create audit_payload with the success information.
    Parameters:
        config_object - configuration object to get the ingestion execution information
    Returns:
        dict : audit_payload with the success information
    """
    audit_payload = {
        "job_run_id": "",
        "job_name": config_object.get("job_name"),
        "job_id": config_object.get("job_id"),
        "job_start_time": config_object.get("start_ts"),
        "src_obj_name": config_object['inp_bucket']+"/"+config_object['inp_location'],
        "tgt_obj_name": config_object["out_bucket"] + "/" + config_object["out_location"],
        "src_rec_count": config_object.get("count"),
        "tgt_rec_count": config_object.get("count"),
        "error_message": "",
        "error_type": "",
        "error_obj_name": "",
        "error_obj_loc": "",
        "flag": "SUCCESS",
        "out_bucket": config_object.get("out_bucket"),
        "audit_log_dir": config_object.get("audit_log_dir")}
    audit_payload["audit_func"] = config_object.get("audit_func")
    return s3_audit_log(audit_payload)

def error_payload_builder(config_object):
    """
    This function is used to create audit_payload with the error information.
    Parameters:
        cnf_obj - payload for auditing in case of exceptions
        exc_info - the execution information to get the error object.
    Returns:
        audit_payload with the error information
    """
    try:
        audit_payload = {
            "job_run_id": "",
            "job_name": config_object.get("job_name"),
            "job_id": config_object.get("job_id"),
            "job_start_time": config_object.get("start_ts"),
            "src_obj_name": "",
            "tgt_obj_name": "",
            "src_rec_count": 0,
            "tgt_rec_count": 0,
            "error_message": config_object.get("error_message"),
            "error_type": config_object.get("error_type"),
            "error_obj_name": "",
            "error_obj_loc": "",
            "flag": "FAIL",
            "out_bucket": config_object.get("out_bucket"),
            "audit_log_dir": config_object.get("audit_log_dir"),
            "audit_func": config_object.get("audit_func")
        }
        s3_audit_log(audit_payload)
        return True
    except Exception as e:
        print('Error in building error payload:', str(e))
        return False

def s3_audit_log(audit_payload):
    """
    This function is used to create audit entries into the S3 bucket
    using the lambda function edb-audit-log.
    Parameters:
        audit_payload - payload for auditing
    """
    #lambda_client = boto3.client('lambda', region_name='us-east-2')
    # invoke_response = lambda_client.invoke(FunctionName=audit_payload["audit_func"],
    #                                       InvocationType='RequestResponse',
    #                                       Payload=json.dumps(audit_payload))
    #print("Job execution successful :", invoke_response['Payload'].read())
    print("Job Ran Successfully : Payload is ", audit_payload)
    return True

def has_column(df, col):
    """
    This function checks if column exists in dataframe
    Paramters:
        df - input dataset
        col - column name
    Return:
        bool - True/False
    """
    try:
        df[col]
        return True
    except AnalysisException:
        return False

def select_cols(df, cols):
    """
    Function that adds empty column if dataframe does not have it.
    """
    for column in cols:
        if has_column(df, column):
            df = df.withColumn(column, col(column))
        else:
            df = df.withColumn(column, lit(None).cast("string"))
    return df

def _is_struct(dtype):
    """Function to check if input is StructType"""
    return True if dtype.startswith("struct") else False

def _is_array(dtype):
    """Function to check if input is array"""
    return True if dtype.startswith("array") else False

def _is_map(dtype):
    """Function to check if input is map"""

    return True if dtype.startswith("map") else False

def _is_array_or_map(dtype):
    """Function to check if input is array or map"""

    return True if (dtype.startswith("array") or dtype.startswith("map")) else False

def _parse_aux(path, aux):
    if ":" in aux:
        path_child, dtype = aux.split(sep=":", maxsplit=1)
    else:
        path_child = "element"
        dtype = aux
    return path + "." + path_child, dtype

def _flatten_struct_column(path, dtype):
    dtype = dtype[7:-1]  # Cutting off "struct<" and ">"
    cols = []
    struct_acc = 0
    path_child = ""
    dtype_child = ""
    aux = ""
    for c, i in zip(dtype, range(len(dtype), 0, -1)):  # Zipping a descendant ID for each letter
        if ((c == ",") and (struct_acc == 0)) or (i == 1):
            if i == 1:
                aux += c
            path_child, dtype_child = _parse_aux(path=path, aux=aux)
            if _is_struct(dtype=dtype_child):
                cols += _flatten_struct_column(path=path_child, dtype=dtype_child)  # Recursion
            elif _is_array(dtype=dtype):
                cols.append((path, "array"))
            else:
                cols.append((path_child, dtype_child))
            aux = ""
        elif c == "<":
            aux += c
            struct_acc += 1
        elif c == ">":
            aux += c
            struct_acc -= 1
        else:
            aux += c
    return cols

def _flatten_struct_dataframe(df, explode_outer, explode_pos):
    explode = "EXPLODE_OUTER" if explode_outer is True else "EXPLODE"
    explode = "POS" + explode if explode_pos is True else explode

    cols = []
    for path, dtype in df.dtypes:
        if _is_struct(dtype=dtype):
            cols += _flatten_struct_column(path=path, dtype=dtype)
        elif _is_array(dtype=dtype):
            cols.append((path, "array"))
        elif _is_map(dtype=dtype):
            cols.append((path, "map"))
        else:
            cols.append((path, dtype))
    cols_exprs = []
    expr = ""
    for path, dtype in cols:
        path_under = path.replace('.', '_')
        if _is_array(dtype):
            if explode_pos:
                expr = explode + "(" + path + ") AS (" + path_under + "_pos, " \
                    + path_under + ")"
            else:
                expr = explode + "(" + path + ") AS " + path_under
        elif _is_map(dtype):
            if explode_pos:
                expr = explode + "(" + path + ") AS " + path_under + "_pos, " \
                    + path_under + "_key, " + path_under + "_value)"
            else:
                expr = explode + "(" + path + ") AS " + path_under + "_key, " \
                    + path_under + "_value)"
        else:
            expr = path + " AS " + path.replace('.', '_')
        cols_exprs.append((path, dtype, expr))
    return cols_exprs

def _build_name(name, expr):
    suffix = expr[expr.find("(") + 1:expr.find(")")]
    return (name + "_" + suffix).replace(".", "_")

def flatten(dataframe, explode_outer, explode_pos, name):
    """
    Convert a complex nested DataFrame in one (or many) flat DataFrames
    If a columns is a struct it is flatten directly.
    If a columns is an array or map, then child DataFrames are created in
    different granularities.
    :param dataframe: Spark DataFrame
    :param explode_outer: Should we preserve the null values on arrays?
    :param explode_pos: Create columns with the index of the ex-array
    :param name: The name of the root Dataframe
    :return: A dictionary with the names as Keys and the DataFrames as Values
    """
    cols_exprs = _flatten_struct_dataframe(df=dataframe,
                                           explode_outer=explode_outer,
                                           explode_pos=explode_pos)
    exprs_arr = [x[2] for x in cols_exprs if _is_array_or_map(x[1])]
    exprs = [x[2] for x in cols_exprs if not _is_array_or_map(x[1])]
    dfs = {name: dataframe.selectExpr(exprs)}
    exprs = [x[2] for x in cols_exprs if not _is_array_or_map(x[1]) and not x[0].endswith("_pos")]
    for expr in exprs_arr:
        df_arr = dataframe.selectExpr(exprs + [expr])
        name_new = _build_name(name=name, expr=expr)
        dfs_new = flatten(dataframe=df_arr, explode_outer=explode_outer,
                          explode_pos=explode_pos, name=name_new)
        dfs = dict(dfs)
        dfs.update(dfs_new)
    return dfs

def flatten_crosswalk(df_input, drop_column_list):
    """
    This function is a generic function which can flatten any complex netsed json
    structure into a single flat dataframe.
    The function recursively traverses each element in the dataframe and if the element is a
    nested (StructType/Arraytype) structure, explodes or flattens it accordingly.
    N levels of nestings can also be flattened.
    Args:
        df_input (DataFrame): the dataframe to be flattened
        drop_column_list (List): While flattening if any column that need not be flattened in
                                 the dataframe, the column name should be provided and will be
                                 dropped while flattening.
    Raises:
        excep_msg: Any Exception Occured during the flattening is thrown.

    Returns:
        DataFrame: The resultant flattened DataFrame is returned.
    """
    try:
        complex_fields = {
            field.name: field.dataType
            for field in df_input.schema.fields
            if isinstance(field.dataType, (types.ArrayType, types.StructType))
        }
        while len(complex_fields) != 0:
            col_name = list(complex_fields.keys())[0]
            if col_name in drop_column_list:
                df_input = df_input.drop(col_name)
            elif isinstance(complex_fields[col_name], types.StructType):
                expanded = [func.col(col_name + '.' + k).alias(col_name + '_' + k)
                            for k in [n.name for n in  complex_fields[col_name]]]
                df_input = df_input.select("*", *expanded).drop(col_name)
            elif isinstance(complex_fields[col_name], types.ArrayType):
                df_input = df_input.withColumn(col_name, func.explode_outer(col_name))
            complex_fields = {
                field.name: field.dataType
                for field in df_input.schema.fields
                if isinstance(field.dataType, (types.ArrayType, types.StructType))
            }
        return df_input
    except Exception as excep_msg:
        raise excep_msg

def init_dataframes(list_dataframes):
    """
    This function is to create data frame.
    Paramters:
        list_dataframes - list of columns to be created
    """
    try:
        for i in list_dataframes:
            schema = StructType([StructField(c, StringType()) for c in list_dataframes])
        return SPARK.createDataFrame([], schema)
    except Exception as excep_msg:
        raise excep_msg

def etl_ops(inp_df, config_object):
    """
    This function performs ETL transform column operation
    Paramters:
        inp_df - input dataset
        config_object - object with list of columns to be transformed
    Return:
        inp_df - with sensitive columns transformed
    """
    try:
        zip_array = ast.literal_eval(config_object["zip_list"])
        if "dob_col" in config_object or "zipcode_col" in config_object:
            for col_name in inp_df.columns:
                if col_name in config_object["dob_col"]:
                    inp_df = inp_df.withColumn("Age", func.floor(
                        func.datediff(func.current_date(),
                                               func.date_format(col_name,
                                                                "yyyy-MM-dd"))/365))
                    inp_df = inp_df.withColumn("YearOfBirth", func.year(col_name))
                # zip code transformation
                if col_name in config_object["zipcode_col"]:
                    inp_df = inp_df.withColumn("ZipCode",
                                               func.when(func.substring(
                                                   col_name, 0, 3).isin(zip_array),
                                                   '000').otherwise(
                                                   func.substring(
                                                       col_name, 0, 3)))
        # inp_df.show()
        return inp_df
    except:
        get_exception(config_object)

def rename_df(df):
    """
    This function is to rename data frame.
    Paramters:
        df - data aaframe to be renamed
    Return:
        df - dataframe with renamed columns
    """
    for name in df.schema.names:
        df = df.withColumnRenamed(name, name.replace("_tkn", "")
                                  .replace("attributes", "").replace("Addresses", "")
                                  .replace("value", "").replace("Identifiers", "")
                                  .replace("ProgramCampaign", "").replace("_", ""))
    return df

def merge_files(config_object, s3_obj_dict):
    """
    This function merges multiple input files into signle file
    used for processing
    Parameters:
        config_object - job config details
        s3_obj_dict - list of input files to be merged
    Return:
        merged_file_name - location of the merged file
    """

    #s3_cli = boto3.client('s3')
    #s3_res = boto3.resource('s3')
    ts_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    stg_file = config_object['stg_location']+'.json'
    bucket_name = config_object["inp_bucket"]
    json_nl_str = ""
    for key, file in s3_obj_dict.items():
        #obj = s3_cli.get_object(Bucket=bucket_name, Key=file)
        #stream_data = obj['Body'].read().decode('utf-8')
        io_stream = open(file, 'r')
        stream_data = io_stream.read()
        io_stream.close()

        if json_nl_str == "":
            json_nl_str = stream_data
        else:
            json_nl_str = json_nl_str+"\n"+stream_data
    #s3object = s3_res.Object(bucket_name, stg_file)
    # s3object.put(Body=(json_nl_str.encode('UTF-8')))
    
    json_nl_str1 = json.loads(json_nl_str)
    #json_nl_str1 = json.loads(json_nl_str1)
    file_stream = open(stg_file, 'w')
    json.dump(json_nl_str1, file_stream)
    # file_stream.write(json_nl_str)
    file_stream.close()
    #inp_file = "s3://"+bucket_name+"/"+stg_file
    config_object['inp_read_path'] = stg_file
    return config_object

def get_s3_obj_dict(config_object):
    try:
        #conn = boto3.client('s3')
        s3_obj_dict = {}
        itr_val = 0
        # list_obj = conn.list_objects(Bucket=config_object.get("inp_bucket"),
        #                             Prefix=config_object.get("inp_location"))
        '''
        if 'Contents' in list_obj:
            for obj_name in list_obj['Contents']:
                if "processed" not in obj_name['Key']:
                    file_name = obj_name['Key']
                    s3_obj_dict[itr_val] = obj_name['Key']
                    itr_val += 1
        '''
        dict1 = []
        for obj_name in os.listdir('C:/Local_Debug/'+config_object["inp_location"]):
            s3_obj_dict[itr_val] = config_object["inp_location"] + obj_name
            itr_val += 1

        # print(s3_obj_dict)
        '''s3_obj_dict1 = {0:"pph_iden/reltio_patient_outbound/data/patient_consent/incoming_data/entity_changed_program.json"}            
        s3_obj_dict1'''
        return s3_obj_dict
    except:
        get_exception(config_object)
    #return {}

def build_conf_obj(bucket, key, param_list):
    """
    This function is used to build config object from config file.
    Parameters:
        bucket - s3 bucket name.
        key - config file path along with name.
        param_list - list of sections to fetch from param file.
    Returns:
        dict : config_object dictionary.
    """
    try:
        start_time = datetime.datetime.fromtimestamp(time.time()) \
            .strftime('%Y-%m-%d %H:%M:%S')
        partition_ts = datetime.datetime.now().strftime('%Y%m')
        #s3_client = boto3.client('s3')
        #obj = s3_client.get_object(Bucket=bucket, Key=key)
        #config_resp = obj['Body'].read().decode('utf-8')
        io_stream = open(key, 'r')
        config_resp = io_stream.read()
        io_stream.close()
        config_object = {}
        if config_resp != "":
            json_config = json.loads(config_resp)
            config_object = json_config[param_list]
            config_object["start_ts"] = start_time
            config_object["partition_ts"] = partition_ts
            return config_object
    except Exception as exp_msg:
        print("Job terminated.Error in building config file :", exp_msg)
        sys.exit(1)

def process_hco_address(config_object_hco_address):
    """
    The function reads files stored at S3 location as mentioned in config file.
    It performs deidentification on the data and store is at s3 output
    location.
    Parameters:
        config object
    Returns:
        None
    """
    try:
        s3_obj_dict = get_s3_obj_dict(config_object_hco_address)
        if bool(s3_obj_dict):
            config_object_hco_address = merge_files(config_object_hco_address, s3_obj_dict)
            input_dataframe = SQLCONTEXT.read.json(config_object_hco_address.get("inp_read_path"))
            address_data = input_dataframe.select("attributes.Address")
            
            list_address = config_object_hco_address.get("address_single")
            list_brick = config_object_hco_address.get("address_brick_nested")
            list_zip = config_object_hco_address.get("address_zip")
            list_original_zip = config_object_hco_address.get("address_original_zip")
            
            df_crosswalk_flattened = flatten_crosswalk(input_dataframe.select('attributes.Address.refRelation.crosswalks'), [])
            df_attribute_flattened = flatten(address_data, True, True, "root")
            
            list_address = list_address.replace("'", "").replace("[","").replace("]","").split(",")
            list_brick = list_brick.replace("'", "").replace("[","").replace("]","").split(",")
            list_zip = list_zip.replace("'", "").replace("[","").replace("]","").split(",")
            list_original_zip = list_original_zip.replace("'", "").replace("[","").replace("]","").split(",")
            
            init_dataframes(list_address)
            
            for name, df_flat in df_attribute_flattened.items():
                name_suffix = name.split("_")[len(name.split("_"))-1]  
                if "Address" in name and "BRICK" in name and name_suffix in list_brick:
                    globals()[name_suffix] = df_flat
                    split_col = F.split(df_flat["Address_value_BRICK_value_" + name_suffix + "_uri"], "/")
                    if df_crosswalk_flattened.filter("crosswalks_attributeURIs like 'split_col'"):
                        uri = df_flat["Address_value_BRICK_value_" + name_suffix + "_uri"]
                        value =  df_flat["Address_value_BRICK_value_" + name_suffix + "_value"]
                        #source_sys = df_crosswalk_flattened["crosswalks_type"].split("/")[-1]
                elif "Address" in name and "OriginalZip" in name and name_suffix in list_original_zip:
                    globals()[name_suffix] = df_flat
                    split_col = F.split(df_flat["Address_value_OriginalZip_value_" + name_suffix + "_uri"], "/")
                    if df_crosswalk_flattened.filter("crosswalks_attributeURIs like 'split_col'"):
                        uri = df_flat["Address_value_OriginalZip_value_" + name_suffix + "_uri"]
                        value =  df_flat["Address_value_OriginalZip_value_" + name_suffix + "_value"]
                        #source_sys = df_crosswalk_flattened["crosswalks_type"].split("/")[-1]
                elif "Address" in name and "Zip" in name and name_suffix in list_zip:
                    globals()[name_suffix] = df_flat
                    split_col = F.split(df_flat["Address_value_Zip_value_" + name_suffix + "_uri"], "/")
                    if df_crosswalk_flattened.filter("crosswalks_attributeURIs like 'split_col'"):
                        uri = df_flat["Address_value_Zip_value_" + name_suffix + "_uri"]
                        value =  df_flat["Address_value_Zip_value_" + name_suffix + "_value"]
                        #source_sys = df_crosswalk_flattened["crosswalks_type"].split("/")[-1]
                elif "Address" in name and  name_suffix in list_address:
                    globals()[name_suffix] = df_flat
                    split_col = F.split(df_flat["Address_value_" + name_suffix + "_uri"], "/")
                    if df_crosswalk_flattened.filter("crosswalks_attributeURIs like 'split_col'"):
                        uri = df_flat["Address_value_" + name_suffix + "_uri"]
                        value =  df_flat["Address_value_" + name_suffix + "_value"]
                        #source_sys = df_crosswalk_flattened["crosswalks_type"].split("/")[-1]
                #To complete for single level in if section
    except:
        get_exception(config_object_hco_address)

def main():
    """
    This function acts as a main function
    Parameters:
        None
    Returns:
        None
    """
    #args = getResolvedOptions(sys.argv, ['JOB_NAME', 'config_bucket', 'config_file'])
    args = {"JOB_NAME": "eph_etl", "config_bucket": "lly-edb-raw-dev", "config_file": "config/hco_address_config.json"}
    bucket_name = args['config_bucket']
    config_file = args['config_file']
    config_object_hco_address = build_conf_obj(bucket_name, config_file, "address_hco")
    config_object_hco_address["job_name"] = args['JOB_NAME']
    process_hco_address(config_object_hco_address)
    SPARK.stop()

if __name__ == "__main__":
    main()
