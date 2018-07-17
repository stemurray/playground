import json
import boto3

def ecs_task_state_change_to_ddb_handler(event, context):

    # For debugging so you can see raw event format.
    # print('Here is the event:')
    # print(json.dumps(event))

    # Check corret event source
    if event["source"] != "aws.ecs":
       raise ValueError("Function only supports input from events with a source type of: aws.ecs")

    # Check correct event type
    if event["detail-type"] == "ECS Task State Change":
        print(event["id"] + " : Storing event ")
    elif event["detail-type"] == "ECS Container Instance State Change":
        raise ValueError("Unsupported detail-type")
    else:
        raise ValueError("Unknown detail-type")

    # Initialise DDB table parameters
    print(event["id"] + " : Initialising parameters")
    region_name = "eu-west-1"
    table_name = "ECSTaskStateChanges"
    partition_key_name = "taskArn"
    sort_key_name = "eventVersion"
    event_arn = event["detail"]["taskArn"]
    event_version = event["detail"]["version"]
    
    # Check event is unique
    print(event["id"] + " : Checking if record is already in DDB")
    dynamodb = boto3.resource("dynamodb", region_name=region_name)
    table = dynamodb.Table(table_name)
    saved_event = table.get_item(
        Key={
            parition_key_name : event_arn,
            sort_key_name : event_version
        }
    )

    if "Item" in saved_event:
           raise ValueError("Event is a duplicate")

    # Store new event
    print(event["id"] + " : Saving new record version " + event_version)
    new_record = {}
    new_record["eventVersion"] = event_version
    new_record.update(event["detail"])
 
    table.put_item(
        Item=new_record
    )
    print(event["id"] + " : New record version " + event_version + " saved")
