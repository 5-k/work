'''
Import Libraries
'''
import json
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructField, StringType, StructType
from pyspark.sql import types
from pyspark.sql import Column
from pyspark.sql import SQLContext
from awsglue.utils import getResolvedOptions
import boto3
from botocore.exceptions import ClientError


spark = SparkSession.builder.appName('ecdp_hcp_flatten').config("spark.sql.codegen.wholeStage","false").getOrCreate()
LAMBDA_CLIENT = boto3.client("lambda", region_name="us-east-2")
s3 = boto3.client('s3')
S3_RES = boto3.resource('s3')

createdDate = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
updatedDate = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def load_config():

    """
    This function reads the config file the local directory


    Returns:
    config Variable with json data read from Config file.
    """
    global config

    config_data = s3.get_object(Bucket='lly-future-state-arch-poc-dev', Key='Config/config_hcp.json')
    file_lines = config_data['Body'].read().decode('utf-8')

    config = json.loads(file_lines)


def has_column(df_input, column_name):
    """
    This function checks whether the Dataframe has the specified
    column. If yes returns True else False
    Args:
        df_input (Dataframe): the Dataframe where the column name is checked
        column_name (string): Column Name to check fo in the Dataframe

    Returns:
        True/False: Based on whether the column exists or not returns a boolean value
    """
    try:
        return isinstance(df_input[column_name], Column)
    except AnalysisException:
        return False
    except Exception as excep_msg:
        raise excep_msg

def create_schema(column_list):
    """
    This function is used to craete a schema from a column list where
    every column would be of type String
    Args:
        column_list (List): This list contains a list of all columns
                            in the expected dataframe

    Raises:
        Exception: Any exception occured during function execution is raised

    Returns:
        schema: A Dataframe schema which conatins a schema of all the columns in the dataframe
    """
    try:
        schema = ''
        for i in column_list:
            schema = schema + "StructField('" + i + "'" +',StringType(), True),'
        custom_schema = "StructType([" + schema.rstrip(',') + "])"
        df_schema = eval(custom_schema)
        return df_schema
    except Exception as excep_msg:
        raise excep_msg

def flatten(df_input, drop_column_list):
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
                            for k in [n.name for n in complex_fields[col_name]]]
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


def transformation(df_transform,val,transform_type,config):
    """
    Description: Transforming Single and Nested attributes to rows
    based on joined column value aggregation
    :param df_transform:
    :param transform_type:
    :return:
    """
    try:

        if transform_type == 'SINGLE':
         
            column_list = [k for k in config['HCP']]
            row_val = [n for n in config['joinedcol']]
            df_schema = create_schema(column_list)
            df_trans = spark.createDataFrame([], schema=df_schema)

            cols = [func.when(func.col("joined_column") == m, \
                              func.col("value")).otherwise(None).alias(m) \
                    for m in row_val]
            maxs = [func.max(func.col(m)).alias(m) for m in row_val]

            df_transform = df_transform.select(func.col("cross_source"), func.col("cross_value"),func.col("EntityID"),\
                                        func.col("sourceid"),*cols)\
                                        .groupBy("cross_source", "cross_value","sourceid","EntityID") \
                                        .agg(*maxs) \
                                        .na.fill(0)
                                         
            df_trans= df_trans.unionByName(df_transform)
         
        else:

            df_transform = df_transform.withColumn("cross_uri", \
                                                   func.split(df_transform['uri'], '/')[4])\
                .drop(df_transform['uri'])
                
            row_val = [i.split('.')[-1] for i in val]
 
            row_val1=list(row_val)
            row_val.extend(["EntityID","cross_source","cross_uri","cross_value"])
            column_list=row_val
            df_schema = create_schema(column_list)

            df_trans = spark.createDataFrame([], schema=df_schema)
        
            cols = [func.when(func.col("joined_column") == m, \
                              func.col("value")).otherwise(None).alias(m)
                    for m in row_val1]
            maxs = [func.max(func.col(m)).alias(m) for m in row_val1]

            df_transform = (df_transform
                        .select(func.col("cross_source"), func.col("cross_value"),func.col("EntityID"), \
                                func.col("cross_uri"), *cols)\
                        .groupBy("cross_source", "cross_uri", "cross_value","EntityID")\
                        .agg(*maxs)
                        .na.fill(0))
            df_trans = df_trans.unionByName(df_transform)
   
        return df_trans


    except Exception as excep_msg:
        return excep_msg
        
def joined_single_nested(df_s, df_n,val):
    
    col_list =['HCPUniqueId','cross_source']
    row_val = [i.split('.')[-1] for i in val]
    col_list.extend(row_val)

    column_list=col_list
    df_schema = create_schema(column_list)
    df_nested_f = spark.createDataFrame([], schema=df_schema)
    
    
    df_final = df_s.alias('df1').join(df_n.alias('df2'), ['EntityID','cross_value'],'inner').select('df1.HCPUniqueId','df2.*') \
              .drop('cross_value', 'cross_uri', 'EntityID')
  
    df_nested_f = df_nested_f.unionByName(df_final)
    df_nested_f = df_nested_f.withColumn('createdDate',func.lit(createdDate))
    df_nested_f = df_nested_f.withColumn('updatedDate',func.lit(updatedDate)) 
       
    return df_nested_f        
    
def joined_atribute_s(df_cross,df_attribute,config):

    """
    :param df_input:
    :param df_cross:
    :param df_attribute:
    :param df_nested_att:
    :return:
    """
    try:

        df_single_att = df_cross.join(df_attribute, df_cross['uri'] == df_attribute['uri'], 'inner') \
            .select(df_cross['cross_value'], df_cross['cross_source'], df_cross['uri'],df_cross['sourceid'],\
                    df_attribute['value'], df_attribute['joined_column'],df_attribute['EntityID'])
      
        df_single = transformation(df_single_att,[],'SINGLE',config)
        df_single=df_single.drop('HCPReferenceId')


        return df_single

    except Exception as excep_msg:
        raise excep_msg

def joined_atribute_n(df_cross,df_nested_att,val,config):

    """

    :param df_input:
    :param df_cross:
    :param df_attribute:
    :param df_nested_att:
    :return:
    """
    try:

        df_nested_att = df_cross.join(df_nested_att, df_cross['uri'] == df_nested_att['uri'], 'inner') \
            .select(df_cross['cross_value'], df_cross['cross_source'], df_cross['uri'], \
                    df_nested_att['value'], df_nested_att['joined_column'],df_nested_att['EntityID'])

        df_nested = transformation(df_nested_att,val, None,config)

        return df_nested

    except Exception as excep_msg:
        raise excep_msg

def df_crosswalk_flattened(df_input,config):
    """
      Description: Crosswalks flattening
      Input: df_input dataframe
      Output: df_attribute dataframe contains uri, cross_value and crosswalk source
      """
    try:

        df_schema = StructType([
            StructField("uri", StringType(), True), \
            StructField("sourceid", StringType(), True),\
            StructField("cross_value", StringType(), True), \
            StructField("cross_source", StringType(), True)
        ])

        df_crosswalk = spark.createDataFrame([], schema=df_schema)

        df_crosswalk_flattened = flatten(df_input.select('crosswalks'), \
                                         ['crosswalks_singleAttributeUpdateDates'])

        for column in config['crosswalkcol']:
            df_crosswalk_flattened = df_crosswalk_flattened.drop(column)

        df_crosswalk_flattened = df_crosswalk_flattened.\
            withColumn('cross_value',\
                       func.split(df_crosswalk_flattened.crosswalks_uri,'/')[3]) \
            .drop(df_crosswalk_flattened.crosswalks_uri)

        df_crosswalk_flattened = df_crosswalk_flattened.withColumn('cross_source',
                                                                   func.split(df_crosswalk_flattened.crosswalks_type,
                                                                              '/')[2]) \
            .drop(df_crosswalk_flattened.crosswalks_type)

        df_crosswalk = df_crosswalk.unionAll(df_crosswalk_flattened)

        return df_crosswalk

    except Exception as excep_msg:
        raise excep_msg


def df_nested_flattened(df_input,value):
    """
      Description: Attribute level flattening for the nested attributes
      Input: df_input dataframe
      Output: df_nes dataframe contains uri, value and joined column name

      """
    try:

        df_schema = StructType([
            StructField("uri", StringType(), True), \
            StructField("value", StringType(), True), \
            StructField("joined_column", StringType(), True),\
            StructField("EntityID", StringType(), True)
        ])

        def_nes = spark.createDataFrame([], schema=df_schema)
        
        for nestatt in value:
            if has_column(df_input, nestatt):
                df_nest_flattend = flatten(df_input.select(nestatt), [])
                nestatt = nestatt.split('.')[-1]
 
                for val in df_nest_flattend.columns:
                    
                    if 'lookupCode' in val:  
                        selectcols = [nestatt + '_uri', nestatt + '_lookupCode']
                        break
                    else:
                        selectcols = [nestatt + '_uri', nestatt + '_value'] 
                        break   

                
                df_nest_flattend = df_nest_flattend.select(*selectcols) \
                    .withColumnRenamed(selectcols[0], "uri") \
                    .withColumnRenamed(selectcols[1], "value")
                
                df_nest_flattend = df_nest_flattend.withColumn('joined_column', \
                                                                func.split(df_nest_flattend.uri, '/')[5])
                df_nest_flattend = df_nest_flattend.withColumn('EntityID', \
                                                                func.split(df_nest_flattend.uri, '/')[1])


                def_nes = def_nes.unionAll(df_nest_flattend)

        return def_nes

    except Exception as excep_msg:
        raise excep_msg    
        
def df_attribute_flattened(df_input,config):

    """
    Description: Attribute level flattening for the single attributes
    Input: df_input dataframe
    Output: df_attr dataframe contains uri, value and joined column name

    """
    try:

        df_schema = StructType([
            StructField("uri", StringType(), True), \
            StructField("value", StringType(), True), \
            StructField("joined_column", StringType(), True),
            StructField("EntityID", StringType(), True)
        ])
        
        df_attr = spark.createDataFrame([], schema=df_schema)

        for flatatt in config['singleattribute']:
            if has_column(df_input, flatatt):
                df_attr_flattend = flatten(df_input.select(flatatt), [])
                attr = flatatt.split('.')[-1]
                for val in df_attr_flattend.columns:

                    if 'lookupCode' in val:
                        selectcols = [attr + '_uri', attr + '_lookupCode']
                        break
                    else:
                        selectcols = [attr + '_uri', attr + '_value'] 
                        break   
                if attr == 'HCPReferenceId':
                    selectcols.append(attr + '_ov')
                    df_attr_flattend = df_attr_flattend.select(*selectcols).filter(
                        df_attr_flattend[selectcols[2]] == 'true') \
                        .withColumnRenamed(selectcols[0], "uri") \
                        .withColumnRenamed(selectcols[1], "value") \
                        .withColumnRenamed(selectcols[2], "ovvalue")
                    df_attr_flattend= df_attr_flattend.drop('ovvalue')
                    df_attr_flattend = df_attr_flattend.withColumn('joined_column', \
                                                                   func.split(df_attr_flattend.uri, '/')[3])
                    df_attr_flattend_ref_id = df_attr_flattend.withColumn('EntityID', \
                                                                   func.split(df_attr_flattend.uri, '/')[1])
                                                                   
                    continue
                else:
                    df_attr_flattend = df_attr_flattend.select(*selectcols) \
                        .withColumnRenamed(selectcols[0], "uri") \
                        .withColumnRenamed(selectcols[1], "value")
                    df_attr_flattend = df_attr_flattend.withColumn('joined_column', \
                                                                   func.split(df_attr_flattend.uri, '/')[3])
                    df_attr_flattend = df_attr_flattend.withColumn('EntityID', \
                                                                   func.split(df_attr_flattend.uri, '/')[1])


                df_attr = df_attr.union(df_attr_flattend)

        return df_attr,df_attr_flattend_ref_id
    except Exception as excep_msg:
        raise excep_msg
        
def joined_single_refid(df_s,df_ref_single,config):

    try:
        column_list = [k for k in config['HCP']]
        df_schema = create_schema(column_list)
        df_s_final = spark.createDataFrame([], schema=df_schema)
        
        
        df_single_final=df_s.join(df_ref_single,['EntityID'],'inner').select(df_s['*'],df_ref_single['value'].alias('HCPReferenceId'))
    

        df_s_final =df_s_final.unionByName(df_single_final)
        df_s_final = df_s_final.withColumn('createdDate',func.lit(createdDate))
        df_s_final = df_s_final.withColumn('updatedDate',func.lit(updatedDate))
        
        return df_s_final
       
    except Exception as excep_msg:
        raise excep_msg

def archive_s3_files(bucket_name, s3_obj_dict, old_value, new_value):
    """
    Function to archive files to processed folder in S3.
    Parameters:
        bucket_name(string): name of S3 bucket.
        s3_obj_dict(dict)  : list of S3 files to be archived.
    """
    try:
        file_name =[]
        for key, value in s3_obj_dict.items():
            old_file_name = value
            new_file_name = value.replace(old_value, new_value)
            copy_source = {
                'Bucket': bucket_name,
                'Key': old_file_name
            }
            S3_RES.meta.client.copy(copy_source, bucket_name, new_file_name)
            s3.delete_object(Bucket=bucket_name, Key=old_file_name)
            file_name.append(new_file_name)

        return file_name

    except ClientError as err:
        print('Failed to archive files :', err)   

def get_s3_file(config,filter_list,inp_prefix_dir):
    
    try:
        s3_obj_dict={}
        itr_val = 0
        list_obj =s3.list_objects(Bucket=config['inp_bucket'],Prefix=inp_prefix_dir)

        if 'Contents' in list_obj:
                for obj_name in list_obj['Contents']:
                    if obj_name['Key'] not in  filter_list:
                        s3_obj_dict[itr_val] = obj_name['Key']
                        itr_val += 1

        return s3_obj_dict
    except:
        return None

def get_file(config):

    try:
        inp_file_dict = get_s3_file(config, [config['inp_location']],config['inp_location'])
        stg_folder_name = config['stg_location'].split('/')[-2]

        if inp_file_dict == {}:
            print("no files in input Location.Processing can start with files in Input Location")
        else:
            print("Processing Files in input Location {0}".format(config['inp_location']))

            input_file_list = list(inp_file_dict.values())

            if len(input_file_list) <= config['chunk_size']:

                print("Moving the following files {0} to staging location for processing"\
                    .format(input_file_list))

                file_nm = archive_s3_files(config['inp_bucket'], inp_file_dict,\
                "incoming", stg_folder_name)

                return file_nm

            else:
                chunk_size_file_list = input_file_list[: config['chunk_size']]
                chunk_size_dict = {i:v for i, v in enumerate(chunk_size_file_list)}
                print("Moving the following files {0} to staging location for processing"\
                    .format(chunk_size_file_list))

                file_nm= archive_s3_files(config['inp_bucket'], chunk_size_dict,\
                    "incoming", stg_folder_name)

                return file_nm                   

    except Exception as excep_msg:
      raise excep_msg  



def get_spark_executors_information(config):
    """
    Method to fetch spark executors, cores based on the number of DPU's alocated to
    the Glue Job
    Return:
        total_executors: no of executors
        total_cores: no of cores
        total_partition_revised: no of executors* no of cores
    """
    try:
        glue_client = boto3.client('glue', region_name=config.get('region_name',\
            'us-east-2'))
        response = glue_client.get_job_run(JobName=config['job_name'],\
            RunId=config['job_run_id'])
        print("Response:", response)
        print("WorkerType:", response['JobRun'].get('WorkerType'))
        print("Max Capacity:", response['JobRun'].get('MaxCapacity', "Failed to fetch"))
        if response['JobRun'].get('WorkerType', 'Standard') == "Standard":
            total_dpus = int(response['JobRun'].get('MaxCapacity', 5))
            total_executors = (total_dpus - 1)*2 - 1
            total_cores = 4
        elif response['JobRun'].get('WorkerType') == "G.1X":
            total_dpus = response['JobRun'].get('NumberOfWorkers', 5)
            total_executors = total_dpus - 1
            total_cores = 8
        elif response['JobRun'].get('WorkerType') == "G.2X":
            total_dpus = response['JobRun'].get('NumberOfWorkers', 5)
            total_executors = total_dpus - 1
            total_cores = 16
        total_paritions_revised = total_cores * total_executors
        return total_executors, total_cores, total_paritions_revised
    except Exception as excep_msg:
        raise excep_msg                
        
def build_config_object(job_name, batch_name, param_name):
    """
    This function is used to build the config object. Function invokes the lambda function
    edb_abc_fetch_param_values with the job_name and batch_name as the test event which returns
    the json value inserted for the specific param group name.
    Args:
        job_name ([String]): [Job Name of the function]
        batch_name ([String]): [Batch Name the job belongs to]
        param_name ([String]): [The Param name used to fetch the config obj]

    Raises:
        Exception: [Any exception occured during the program execution is raised]

    Returns:
        [Dict]: [Returns the Dict with the param name key and its value]
    """
    try:
        if job_name is None or batch_name is None or param_name is None:
            raise Exception('Cannot build the config object. Mandatory values job_name {0}\
             or batch_name {1} or param_name {2} cannot be null'\
                 .format(job_name, batch_name, param_name))
        fetch_params_payload = {'batch_name': batch_name, 'job_name': job_name}
        response = LAMBDA_CLIENT.invoke(FunctionName='edb_abc_fetch_param_values', \
                                        InvocationType='RequestResponse',\
                                        Payload=json.dumps(fetch_params_payload))
        response_payload = json.loads(response['Payload'].read().decode('utf-8'))
        if "errorMessage" in response_payload:
            raise Exception('Lambda function {0} failed.Error Occured while fetching param values\
                             ErrorMessage: {1}'.format('edb_abc_fetch_param_values',\
                                                       response_payload))
        if param_name not in response_payload:
            raise Exception('Param Name {0} does not exist in resposne payload from Lambda'\
                    .format(param_name))
        return response_payload.get(param_name)
    except Exception as excep_msg:
        raise excep_msg 

def main():
    """
    Calling functions
    """
    try:
        
        config = None
        args = getResolvedOptions(sys.argv, ['JOB_NAME', 'job_name',\
            'batch_name', 'param_name'])
        config = build_config_object(args['job_name'], args['batch_name'],\
            args['param_name']) 

        config['region_name'] = 'us-east-2'
        config['job_name'] = args['JOB_NAME']
        config['job_run_id'] = args['JOB_RUN_ID']  
        total_executors, total_cores, total_partition_revised = \
            get_spark_executors_information(config)
        config['total_executors'] = total_executors
        config['total_cores'] = total_cores
        config['total_partition_revised'] = total_partition_revised

        process_file_nm = get_file(config)

        print('Files ready for processing')

        input_path = 's3://' + config['inp_bucket'] + '/' 

        json_path = [input_path + i for i in process_file_nm]

        df_input = spark.read.json(json_path,\
                     mode='DROPMALFORMED', multiLine="true")
        
       
        df_cross = df_crosswalk_flattened(df_input,config)
        df_attribute, df_attr_flattend_ref_id = df_attribute_flattened(df_input,config)
        df_s=joined_atribute_s(df_cross, df_attribute,config)
        df_final_single=joined_single_refid(df_s, df_attr_flattend_ref_id,config)

        output_loc = 's3://' + config['inp_bucket'] + '/' + config['out_location']
        
        df_final_single.coalesce(total_executors*4).write.parquet(output_loc + 'hcp/', mode='append')


        for nest_dict,val in config['nested'].items():
            df_nested_att = df_nested_flattened(df_input, val)
            df_nested_att= df_nested_att.na.drop()

            if df_nested_att.rdd.isEmpty():
                continue
            df_n = joined_atribute_n(df_cross, df_nested_att,val,config)
            
            df_final = joined_single_nested(df_s, df_n,val)
           
            df_final.coalesce(total_executors).write.parquet(output_loc + nest_dict + "/", mode='append')


    except Exception as excep_msg:
        if config is None:
            raise Exception('Cannot Build the Config object. Error Message: {0}'\
                .format(excep_msg)) from excep_msg
        raise Exception('Exception Occured. Error Message: {0}'.format(excep_msg))\
            from excep_msg
    

if __name__ == "__main__":
    main()        
