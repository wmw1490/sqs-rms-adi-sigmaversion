import sys
import boto3

def handler(event, context):

    client = boto3.client('sqs')

    response = client.receive_message(
        QueueUrl='https://sqs.us-east-2.amazonaws.com/578839498373/sqs-rms-adi-in',
        AttributeNames=[
            'All',
        ],
        MessageAttributeNames=[
            '',
        ],
        MaxNumberOfMessages=1,
        VisibilityTimeout=123,
        WaitTimeSeconds=10,
        ReceiveRequestAttemptId=''
    )

    # get the body of the message
    body = response.get('Body')
    qsostring = response['Messages'][0]['Body']

 
    # split the QSO string into variables needed for DynamoDB table
    qsolocation, qsodatetime, qsobearing, qsocallsign, qsocmsbytes, qsoseconds, qsodistance, qsofreq, qsogridsquare,qsolastcommand, qsomode, qsomsgrcv, qsomsgsent, qsoradiobytes, gwgridsq, gwcallsign, qsohash = qsostring.split(',')

    # Connect to DynamoDB & QSO table
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('QSO')
    
    # Attempt to insert into DynamoDB table
    try:
        table.put_item(Item={'QSOlocation': qsolocation, 'QSOdatetime': qsodatetime, \
            'QSObearing': qsobearing, 'QSOcallsign': qsocallsign, 'QSOcmsbytes': qsocmsbytes, \
            'QSOseconds': qsoseconds, 'QSOdistance': qsodistance, 'QSOfreq': qsofreq, \
            'QSOgridsquare': qsogridsquare, 'QSOlastcommand': qsolastcommand, \
            'QSOmode': qsomode, 'QSOmsgrcv': qsomsgrcv, 'QSOmsgsent': qsomsgsent, \
            'QSOradiobytes': qsoradiobytes, 'GWgridsq': gwgridsq[0:6], 'GWcallsign': gwcallsign, \
            'QSOhash': qsohash } )          

        try:
            # Delete message from sqs once added to dynamodb
            response = client.delete_message(
            QueueUrl='https://sqs.us-east-2.amazonaws.com/578839498373/sqs-rms-adi-in',
            ReceiptHandle=response['Messages'][0]['ReceiptHandle'])
        except:
            print('**unable to delete SQS message**')
    except:
        # do nothing to recover, just output contents of message for later debug work
        print('Unable to write to DynamoDB')
        print(body)

    return {"message": "Successfully executed"}
