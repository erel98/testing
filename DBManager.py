import logging
import boto3
from botocore.exceptions import ClientError

class DBManager:
    
    
    def create_table(self, table_name, key_schema, attribute_definitions, provisioned_throughput, region):
        
        try:
            dynamodb_resource = boto3.resource("dynamodb", region_name=region)
            print("\ncreating the table {} ...".format(table_name))
            self.table = dynamodb_resource.create_table(TableName=table_name, KeySchema=key_schema, AttributeDefinitions=attribute_definitions,
                ProvisionedThroughput=provisioned_throughput)

            # Wait until the table exists.
            self.table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
            
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def store_an_item(self, region, table_name, item):
        try:
            dynamodb_resource = boto3.resource("dynamodb", region_name=region)
            table = dynamodb_resource.Table(table_name)
            table.put_item(Item=item)
        
        except ClientError as e:
            logging.error(e)
            return False
        return True
     
    def get_an_item(self, region, table_name, key):
        try:
            dynamodb_resource = boto3.resource("dynamodb", region_name=region)
            table = dynamodb_resource.Table(table_name)
            response = table.get_item(Key=key)
            item = response['Item']
        except (ClientError, KeyError) as e:
            logging.error(e)
            return None
        return item
        
        
    def select_all(self, table_name, region):
        dynamodb_resource = boto3.resource("dynamodb", region_name=region)
        table = dynamodb_resource.Table(table_name)
        response = table.scan()
        return response['Items']
    
    
    def update_item(self, table_name, region, key, updateExpression, expressionAttributes):
        dynamodb_resource = boto3.resource("dynamodb", region_name=region)
        table = dynamodb_resource.Table(table_name)
        
        response = table.update_item(
            Key=key,
            UpdateExpression=updateExpression,
            ExpressionAttributeValues=expressionAttributes
        )
        
        return response
        