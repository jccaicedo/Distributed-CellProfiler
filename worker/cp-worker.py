from __future__ import print_function
import boto3
import glob
import json
import logging
import os
import re
import subprocess
import sys 
import time
import watchtower
import string

#################################
# CONSTANT PATHS IN THE CONTAINER
#################################

DATA_ROOT = '/home/ubuntu/bucket'
LOCAL_OUTPUT = '/home/ubuntu/local_output'
PLUGIN_DIR = '/home/ubuntu/CellProfiler-plugins'
QUEUE_URL = os.environ['SQS_QUEUE_URL']
AWS_BUCKET = os.environ['AWS_BUCKET']
LOG_GROUP_NAME= os.environ['LOG_GROUP_NAME']
CHECK_IF_DONE_BOOL= os.environ['CHECK_IF_DONE_BOOL']
EXPECTED_NUMBER_FILES= os.environ['EXPECTED_NUMBER_FILES']
if 'MIN_FILE_SIZE_BYTES' not in os.environ:
    MIN_FILE_SIZE_BYTES = 1
else:
    MIN_FILE_SIZE_BYTES = int(os.environ['MIN_FILE_SIZE_BYTES'])
if 'USE_PLUGINS' not in os.environ:
    USE_PLUGINS = 'False'
else:
    USE_PLUGINS = os.environ['USE_PLUGINS']
if 'NECESSARY_STRING' not in os.environ:
    NECESSARY_STRING = False
else:
    NECESSARY_STRING = os.environ['NECESSARY_STRING']

#################################
# CLASS TO HANDLE THE SQS QUEUE
#################################

class JobQueue():

    def __init__(self, queueURL):
        self.client = boto3.client('sqs')
        self.queueURL = queueURL
    
    def readMessage(self):
        response = self.client.receive_message(QueueUrl=self.queueURL, WaitTimeSeconds=20)
        if 'Messages' in response.keys():
            data = json.loads(response['Messages'][0]['Body'])
            handle = response['Messages'][0]['ReceiptHandle']
            return data, handle
        else:
            return None, None

    def deleteMessage(self, handle):
        self.client.delete_message(QueueUrl=self.queueURL, ReceiptHandle=handle)
        return

    def returnMessage(self, handle):
        self.client.change_message_visibility(QueueUrl=self.queueURL, ReceiptHandle=handle, VisibilityTimeout=60)
        return

#################################
# AUXILIARY FUNCTIONS
#################################


def monitorAndLog(process,logger):
    while True:
        output= process.stdout.readline()
        if output== '' and process.poll() is not None:
            break
        if output:
            print(output.strip())
            logger.info(output)  

def printandlog(text,logger):
    print(text)
    logger.info(text)

#################################
# RUN CELLPROFILER PROCESS
#################################

def runCellProfiler(message):
    #List the directories in the bucket- this prevents a strange s3fs error
    rootlist=os.listdir(DATA_ROOT)
    for eachSubDir in rootlist:
        subDirName=os.path.join(DATA_ROOT,eachSubDir)
        if os.path.isdir(subDirName):
            trashvar=os.system('ls '+subDirName)

    # Configure the logs
    logger = logging.getLogger(__name__)

    # Prepare paths and parameters
    if type(message['Metadata'])==dict: #support for cellprofiler --print-groups output
        if  message['output_structure']=='':
            watchtowerlogger=watchtower.CloudWatchLogHandler(log_group=LOG_GROUP_NAME, stream_name=str(message['Metadata'].values()),create_log_group=False)
            logger.addHandler(watchtowerlogger)
            printandlog('You must specify an output structure when passing Metadata as dictionaries',logger)
            logger.removeHandler(watchtowerlogger)
            return 'INPUT_PROBLEM'
        else:
            metadataID = message['output_structure']
            metadataForCall = ''
            for eachMetadata in message['Metadata'].keys():
                if eachMetadata not in metadataID:
                    watchtowerlogger=watchtower.CloudWatchLogHandler(log_group=LOG_GROUP_NAME, stream_name=str(message['Metadata'].values()),create_log_group=False)
                    logger.addHandler(watchtowerlogger)
                    printandlog('Your specified output structure does not match the Metadata passed',logger)
                    logger.removeHandler(watchtowerlogger)
                else:
                    metadataID = string.replace(metadataID,eachMetadata,message['Metadata'][eachMetadata])
                    metadataForCall+=eachMetadata+'='+message['Metadata'][eachMetadata]+','
            message['Metadata']=metadataForCall[:-1]
    elif 'output_structure' in message.keys():
        if message['output_structure']!='': #support for explicit output structuring
            watchtowerlogger=watchtower.CloudWatchLogHandler(log_group=LOG_GROUP_NAME, stream_name=message['Metadata'],create_log_group=False)
            logger.addHandler(watchtowerlogger)
            metadataID = message['output_structure']
            for eachMetadata in message['Metadata'].split(','):
                if eachMetadata.split('=')[0] not in metadataID:
                    printandlog('Your specified output structure does not match the Metadata passed',logger)
                else:
                    metadataID = string.replace(metadataID,eachMetadata.split('=')[0],eachMetadata.split('=')[1])
            printandlog('metadataID ='+metadataID, logger)
            logger.removeHandler(watchtowerlogger)
        else: #backwards compatability with 1.0.0 and/or no desire to structure output
            metadataID = '-'.join([x.split('=')[1] for x in message['Metadata'].split(',')]) # Strip equal signs from the metadata
    else: #backwards compatability with 1.0.0 and/or no desire to structure output
        metadataID = '-'.join([x.split('=')[1] for x in message['Metadata'].split(',')]) # Strip equal signs from the metadata

    localOut = LOCAL_OUTPUT + '/%(MetadataID)s' % {'MetadataID': metadataID}
    remoteOut= os.path.join(message['output'],metadataID)
    replaceValues = {'PL':message['pipeline'], 'OUT':localOut, 'FL':message['data_file'],
            'DATA': DATA_ROOT, 'Metadata': message['Metadata'], 'IN': message['input'], 
            'MetadataID':metadataID, 'PLUGINS':PLUGIN_DIR }

    # Start loggging now that we have a job we care about
    watchtowerlogger=watchtower.CloudWatchLogHandler(log_group=LOG_GROUP_NAME, stream_name=metadataID,create_log_group=False)
    logger.addHandler(watchtowerlogger)	

    # See if this is a message you've already handled, if you've so chosen
    if CHECK_IF_DONE_BOOL.upper() == 'TRUE':
        try:
            s3client=boto3.client('s3')
            bucketlist=s3client.list_objects(Bucket=AWS_BUCKET,Prefix=remoteOut+'/')
            objectsizelist=[k['Size'] for k in bucketlist['Contents']]
            objectsizelist = [i for i in objectsizelist if i >= MIN_FILE_SIZE_BYTES]
            if NECESSARY_STRING:
                if NECESSARY_STRING != '':
                    objectsizelist = [i for i in objectsizelist if NECESSARY_STRING in i]
            if len(objectsizelist)>=int(EXPECTED_NUMBER_FILES):
                printandlog('File not run due to > expected number of files',logger)
                logger.removeHandler(watchtowerlogger)
                return 'SUCCESS'
        except KeyError: #Returned if that folder does not exist
            pass	
    
    # Build and run CellProfiler command
    cpDone = localOut + '/cp.is.done'
    cp2 = False
    with open(os.path.join(replaceValues['DATA'],replaceValues['PL']), 'r') as openpipe:
        for line in openpipe:
            if 'DateRevision:2' in line: #comes from a CP2 pipeline
                cp2 = True
                cmdstem = 'cellprofiler -c -r -b '
    if not cp2:
        cmdstem = 'cellprofiler -c -r '
    if message['pipeline'][-3:]!='.h5':
        cmd = cmdstem + '-p %(DATA)s/%(PL)s -i %(DATA)s/%(IN)s -o %(OUT)s -d ' + cpDone
        cmd += ' --data-file=%(DATA)s/%(FL)s '
        cmd += '-g %(Metadata)s'
    else:
        cmd = cmdstem + '-p %(DATA)s/%(PL)s -i %(DATA)s/%(IN)s -o %(OUT)s -d ' + cpDone + ' --data-file=%(DATA)s/%(FL)s -g %(Metadata)s'
    if USE_PLUGINS.lower() == 'true':
        cmd += ' --plugins-directory=%(PLUGINS)s'
    cmd = cmd % replaceValues
    print('Running', cmd)
    logger.info(cmd)
    
    subp = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    monitorAndLog(subp,logger)

    # Get the outputs and move them to S3
    if os.path.isfile(cpDone):
        time.sleep(30)
        mvtries=0
        while mvtries <3:
            try:
                    printandlog('Move attempt #'+str(mvtries+1),logger)
                    cmd = 'aws s3 mv ' + localOut + ' s3://' + AWS_BUCKET + '/' + remoteOut + ' --recursive' 
                    subp = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
                    out,err = subp.communicate()
                    printandlog('== OUT \n'+out, logger)
                    if err == '':
                        break
                    else:
                        printandlog('== ERR \n'+err,logger)
                        mvtries+=1
            except:
                printandlog('Move failed',logger)
                printandlog('== ERR \n'+err,logger)
                time.sleep(30)
                mvtries+=1
        if next(open(cpDone))=='Complete\n':
            if mvtries<3:
                printandlog('SUCCESS',logger)
                logger.removeHandler(watchtowerlogger)
                return 'SUCCESS'
            else:
                printandlog('OUTPUT PROBLEM. Giving up on '+metadataID,logger)
                logger.removeHandler(watchtowerlogger)
                return 'OUTPUT_PROBLEM'
        else:
            printandlog('CP PROBLEM: Done file reports failure',logger)
            logger.removeHandler(watchtowerlogger)
            return 'CP_PROBLEM'
    else:
        printandlog('CP PROBLEM: Done file does not exist.',logger)
        logger.removeHandler(watchtowerlogger)
        import shutil
        shultil.rmtree(localOut, ignore_errors=True)
        return 'CP_PROBLEM'
    

#################################
# MAIN WORKER LOOP
#################################

def main():
    queue = JobQueue(QUEUE_URL)
    # Main loop. Keep reading messages while they are available in SQS
    while True:
        msg, handle = queue.readMessage()
        if msg is not None:
            result = runCellProfiler(msg)
            if result == 'SUCCESS':
                print('Batch completed successfully.')
                queue.deleteMessage(handle)
            else:
                print('Returning message to the queue.')
                queue.returnMessage(handle)
        else:
            print('No messages in the queue')
            break

#################################
# MODULE ENTRY POINT
#################################

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    print('Worker started')
    main()
    print('Worker finished')

