#Import Libraries
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
from pyspark.sql.functions import lit,concat
from pyspark.sql.functions import trim
import os
import boto3
import pandas as pd

spark = SparkSession.builder.appName('new').config("spark.sql.broadcastTimeout", "1000").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)
sc.setLogLevel("ERROR")

now = datetime.now()
timestampStr = now.strftime('%Y-%m-%dT%H-%M-%S')

current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)

def load_config(configFilePath,isS3Run):
    """
    This function reads the config file the local directory
    Returns:
    config Variable with json data read from Config file.
    """
    global config
    if not isS3Run:
        with open(configFilePath, encoding='UTF-8') as con:
            config = json.loads(con.read())
    else:
        s3 = boto3.client('s3')
        data = s3.get_object(Bucket='lly-edp-landing-us-east-2-dev', Key='enterprisecustomer/eph/config/'+sys.argv[1])
        file_lines = data['Body'].read().decode('utf-8')
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
 
def transformation(df_transform, val):
    print('hi 1')
    """
    Description: Transforming Single and Nested attributes to rows
    based on joined column value aggregation
    :param df_transform:
    :param transform_type:
    :return:
    """
    try:
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print("transform start time  =", current_time)
        #df_transform = df_transform.withColumn("cross_uri", func.split(df_transform['uri'], '/')[4]).drop(df_transform['uri'])
        
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print("df_transform uri extract  =", current_time)
        
        
        row_val = [n for n in config['joinedcol']]
        
        
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print("row val end time  =", current_time)
        
        
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print("cols end time  =", current_time)
        
        
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print("maxs end time  =", current_time)
        
        df_transform.show(100, False)
        df_trans = df_transform\
                        .groupBy("crosswalks_value","entityID","cross_source")\
                        .pivot("joined_column", row_val)\
                        .agg(func.max(func.col("value")))\
                        .na.fill(0)
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print("df_trans end time  =", current_time)
        
        print('before return')    
        return df_trans
        
    except Exception as excep_msg:
        return excep_msg


def df_crosswalk_flattened(df_input):
    """
      Description: Crosswalks flattening
      Input: df_input dataframe
      Output: df_attribute dataframe contains uri, cross_value and crosswalk source
    """
    try:
        df_schema = StructType([
            StructField("crosswalks_value", StringType(), True), \
            #StructField("cross_value", StringType(), True), \
            StructField("entityid", StringType(), True), \
            StructField("cross_source", StringType(), True)
        ])

        df_crosswalk = sqlContext.createDataFrame(sc.emptyRDD(), schema=df_schema)
        if not has_column(df_input, 'crosswalks'):
            msg = "column crosswalk not found"
            print(msg)
            return None
        
        df_crosswalk_flattened = flatten(df_input.select('crosswalks'), 
                                         ['crosswalks_singleAttributeUpdateDates'])
        
        for column in config['crosswalkcol']:
            df_crosswalk_flattened = df_crosswalk_flattened.drop(column)

        df_crosswalk_flattened = df_crosswalk_flattened.\
            withColumn('cross_value',\
                       func.split(df_crosswalk_flattened.crosswalks_uri,'/')[3]) \
            #.drop(df_crosswalk_flattened.crosswalks_uri)
            
        df_crosswalk_flattened = df_crosswalk_flattened.\
        withColumn('EntityID',\
                   func.split(df_crosswalk_flattened.crosswalks_uri,'/')[1]) \
        .drop(df_crosswalk_flattened.crosswalks_uri)
        
        df_crosswalk_flattened = df_crosswalk_flattened.withColumn('cross_source',
                                                                   func.split(df_crosswalk_flattened.crosswalks_type,
                                                                              '/')[2]) \
            .drop(df_crosswalk_flattened.crosswalks_type)
        df_crosswalk_flattened = df_crosswalk_flattened.drop("cross_value").drop_duplicates()
        
       # df_crosswalk_flattened.persist()
    #    df_crosswalk_flattened.show()

        df_crosswalk = df_crosswalk.unionAll(df_crosswalk_flattened)
        return df_crosswalk
    except Exception as excep_msg:
        raise excep_msg

def  df_address_flatenned(df_cross, df_input, value):
    
    prefixAlreadyFetched = "attributes."

    df_address_data = flatten(df_input.select("attributes.Address") ,[]).na.drop(how="all").distinct()
    
    

    for col in config['addressDropCol']:
       
        if has_column(df_address_data, col):
            
            df_address_data = df_address_data.drop(col)
           
    print('line-218')        
    

    df_address_joinedData = df_cross.join(df_address_data,df_address_data.Address_refRelation_crosswalks_value.contains(df_cross.crosswalks_value), how='inner')\
                .drop_duplicates()\
                .filter(df_address_data.Address_ov==True)
    #print("showing df_address_joinedData /n")
    #df_address_joinedData.persist()
    #df_address_joinedData.show()
                
    df_address_joinedData.drop('Address_refRelation_crosswalks_value')   
    df_address_joinedData.drop('Address_ov')
    #df_address_joinedData.persist()
    #df_address_joinedData.show()
    try:
        df_schema = StructType([
            StructField("crosswalks_value", StringType(), True), \
            StructField("entityid", StringType(), True), \
            StructField("cross_source", StringType(), True), \
            StructField("value", StringType(), True),
            StructField("joined_column", StringType(), True)
        ])
        
        
        def_nes = sqlContext.createDataFrame(sc.emptyRDD(), schema=df_schema)
        #print("showing df_address_joinedData /n")
        #df_address_joinedData.show()
        for dict in value:       
            for key in dict:
                originalKey = key
                key = key[len(prefixAlreadyFetched):].replace('.','_')
                valueToFind = dict[originalKey]              
                fullcolumnName = key + "_" + valueToFind 
                ovColumn = key + "_" + 'ov'
                if has_column(df_address_joinedData, fullcolumnName): 
                    if has_column(df_address_joinedData, ovColumn):
                        #print("if has_column(df_address_joinedData, ovColumn): \n")
                        #df_address_joinedData.filter(df_address_data[ovColumn]==True).show(2, False)
                        df_address_joinedData_new = df_address_joinedData.filter(df_address_data[ovColumn]==True)
                        #print("248 line \n")
                        #df_address_joinedData_new.show(2, False)
                    else:
                        print('missing ov value for column ')
                        print(fullcolumnName)
                        df_address_joinedData_new = df_address_joinedData

                    key = key.split('_')[-1]
                    selectcols = [fullcolumnName]
                    #print("Below are the seleccols")
                    #print(selectcols)
                    
                    #df_address_joinedData_new.show()
                    df_nest_flattend = df_address_joinedData_new.select(func.col("crosswalks_value"),func.col("entityid"),func.col("cross_source"),*selectcols) \
                                        .withColumnRenamed(selectcols[0], "value") 
                    #print("line 273")                    
                    df_nest_flattend = df_nest_flattend.withColumn('joined_column',   func.concat(func.lit(key), func.lit(valueToFind.title())))   
                    
                    def_nes = def_nes.unionAll(df_nest_flattend)
        #df_states = df_states.withColumn('states_Name', trim(df_states.state_name))
        
        #for c_name in def_nes.columns:
         #   def_nes = def_nes.withColumn(c_name, trim(col(c_name)))

        #def_nes.show()
        
        return def_nes
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
            StructField("joined_column", StringType(), True),
            StructField("EntityID", StringType(), True)
        ])

        def_nes = sqlContext.createDataFrame(sc.emptyRDD(), schema=df_schema)
        for dict in value:       
            for key in dict:
                valueToFind = dict[key]              
                fullcolumnName = key + "." + valueToFind 
                if has_column(df_input, fullcolumnName): 
                    df_nest_flattend = flatten(df_input.select(key), [])
                    key = key.split('.')[-1]
                    selectcols = [key + '_uri', key + "_" + valueToFind]
                    df_nest_flattend = df_nest_flattend.select(*selectcols) \
                        .withColumnRenamed(selectcols[0], "uri") \
                        .withColumnRenamed(selectcols[1], "value") 
                    df_nest_flattend = df_nest_flattend.withColumn('joined_column',   func.concat(func.split(df_nest_flattend.uri, '/')[5] , func.lit(valueToFind)))   
                    df_nest_flattend = df_nest_flattend.withColumn('EntityID', \
                                                                    func.split(df_nest_flattend.uri, '/')[1])
                    def_nes = def_nes.unionAll(df_nest_flattend)
        return def_nes
    except Exception as excep_msg:
        raise excep_msg

def df_attribute_flattened(df_input):
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
        df_attr = sqlContext.createDataFrame(sc.emptyRDD(), schema=df_schema)

        for flatatt in config['singleattribute']:
            if has_column(df_input, flatatt):
                df_attr_flattend = flatten(df_input.select(flatatt), [])
                attr = flatatt.split('.')[-1]
                selectcols = [attr + '_uri', attr + '_value']
                df_attr_flattend = df_attr_flattend.select(*selectcols) \
                    .withColumnRenamed(selectcols[0], "uri") \
                    .withColumnRenamed(selectcols[1], "value")
                df_attr_flattend = df_attr_flattend.withColumn('joined_column',\
                                                               func.split(df_attr_flattend.uri, '/')[3])
                df_attr_flattend = df_attr_flattend.withColumn('EntityID', \
                                                               func.split(df_attr_flattend.uri, '/')[1])
                df_attr = df_attr.union(df_attr_flattend)
        return df_attr
    except Exception as excep_msg:
        raise excep_msg

def createDeletFolder(path, createOrDelete):
    if createOrDelete =='delete':
        try:
            os.rmdir(path)
        except OSError:
            print ("Deletion of the directory %s failed" % path)
        else:
            print ("Successfully deleted the directory %s" % path)
    elif createOrDelete =='create':
        try:
            os.mkdir(path)
        except OSError:
            print ("Deletion of the directory %s failed" % path)
        else:
            print ("Successfully deleted the directory %s" % path)

def main():
    """
    Calling functions
    """
    print(sys.argv)
    if len(sys.argv) <= 1 :
        ex_msg = 'Missing Config File Path'
        print(ex_msg)
        raise ex_msg
    
    isS3Run = True
    if( len(sys.argv) == 3  and sys.argv[2] == 'l'):
        isS3Run = False
    
    load_config(sys.argv[1], isS3Run)
    
    if isS3Run:
        df_input = spark.read.option("multiline","true").json(config['s3fileInputPath'])
    else:
        df_input = spark.read.option("multiline","true").json(config['localfileInputPath'])
        createDeletFolder(config['outerFolderPrefix'],'delete')
        createDeletFolder(config['outerFolderPrefix'],'create')
    #print(current_timestamp())
    print(df_input.count())
        
    df_cross = df_crosswalk_flattened(df_input)
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("df_cross endtime =", current_time)
    
    
    #1 For  joining crosswalk and each nested attribute
    for nest_dict,val in config['nested'].items():
        print("Starting processing for key: " + nest_dict)
        
        df_nested_att = df_address_flatenned(df_cross, df_input, val)
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print("df_nested_att endtime =", current_time)
        #df_nested_att.persist()
        #df_nested_att.show(9, False)
        
        print("Transforming Address(" + nest_dict + ") File:\n")
        #df_nested_att.distinct()
        #df_nested_att.persist()
        #df_nested_att.show()
        
        df_n = transformation(df_nested_att, val)
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        print("df_n endtime =", current_time)
        print('transformation completed\n')
        
        
        print("Finished processing for key: " + nest_dict)

        if isS3Run:
            try:
                outputPath = config['outputFolderPath'] +  nest_dict
                
                
                print(outputPath)
                df_n.coalesce(20).write.parquet(outputPath, mode='append')
            except Exception as excep_msg:
                print(excep_msg)            
                print("Empty dataframe for key: " + nest_dict)
            
            
            print("Written the target file")
            #df_cross.unpersist()
            #df_transform.unpersist()
        else:
            try:
                #df_n.coalesce(1).write.parquet("s3://lly-edp-landing-us-east-2-dev/enterprisecustomer/eph/analytic/hco/processed_data/"+ nest_dict+"/",mode='append')
                fileName = config['outerFolderPrefix'] + "/" + nest_dict + '.csv'
                
                
                #df_n.show()
                #df_nd = df_n.toPandas().drop(['cross_uri'], axis = 1)
                #df_n.drop('cross_uri').dropDuplicates()
                print("Dropped uri")
                #df_rd = df_nd.drop_duplicates()
                print("Writing Data For Key: " + nest_dict + " to file: " + fileName)
                #df_n.show()
                #df_n.write.csv(fileName)
                df_n.toPandas().to_csv(fileName)
                
                #df_n.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(fileName)
                print("Completed Writing Data For Key: " + nest_dict )
            except Exception as excep_msg:
                print(excep_msg)            
                print("Empty dataframe for key: " + nest_dict)
        
if __name__ == "__main__":
    main()
