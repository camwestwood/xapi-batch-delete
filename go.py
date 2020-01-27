import os
import argparse
import pandas as pd
import requests
import json
import base64
import dateutil
from datetime import datetime
from laconfig import getLAconfig, postEvent
from logconfig import logger


def date_converter(o):
    if isinstance(o, datetime):
        return o.__str__()


def xapi_batch_delete(deletions, xapi_headers):
    count = 0

    # set up batch delete queries
    body = [
        {
            "filter": {

                "statement.actor.account.name": {
                    "$in": [""]
                }
            },
            "timestamp": {
                "$gt": {
                    "$dte": "2019-11-04T00:00:00"
                },
                "$lt": {
                    "$dte": "2019-11-05T00:00:00"
                }
            }
        }
    ]

    # generate query strings
    for row, student in deletions.iterrows():
        student_id = student['STUDENT_ID']
        vle_id = student['VLE_ID']
        shib_id = student['SHIB_ID']
        student_ids = [student_id, vle_id, shib_id]
        students_upper = [x.upper() for x in student_ids]
        students_lower = [x.lower() for x in student_ids]
        student_ids = student_ids + students_upper + students_lower

        # parse dates for json format
        start_date = student['START_DATE']
        start_date = dateutil.parser.parse(start_date)
        start_date = datetime.strftime(start_date, '%Y-%m-%dT%H:%M:%S')
        end_date = student['END_DATE']
        end_date = dateutil.parser.parse(end_date)
        end_date = datetime.strftime(end_date, '%Y-%m-%dT%H:%M:%S')

        # update query with students and time periods
        body[0]["filter"]["statement.actor.account.name"]["$in"] = student_ids
        body[0]['timestamp']['$gt']['$dte'] = start_date
        body[0]['timestamp']['$lt']['$dte'] = end_date

        str_query = json.dumps(body)
        str_query = str_query[1:-1]

        str_query = json.loads(str_query)

        url = 'https://jisc.learninglocker.net/api/v2/batchdelete/initialise'

        logger.info('Sending batch request: ' + url + str(str_query))
        response = requests.post(url, headers=xapi_headers, json=str_query)

        if response.ok:
            logger.info(str(response) + ': Successfully sent batch delete request')
            count += 1
        else:
            logger.error(str(response) + ': Post statement to LDH failed')

    if count > 0:
        # verify requests
        logger.info('Showing ' + str(count) + ' successful statements sent to LDH')
        request_verify = 'https://jisc.learninglocker.net/api/connection/batchdelete?filter={"done": false}&sort={"createdAt":-1,"_id": 1}&first=**count**'
        request_verify = request_verify.replace('**count**', str(count))
        response = requests.get(request_verify, headers=xapi_headers)

        if response.ok:
            message = json.dumps(response.text)
            logger.info(message)
            # show the user the ids and filters of the requests sent
    else:
        logger.info('No requests sent to LDH, check process.')


parser = argparse.ArgumentParser(description='Process to delete xAPI data when requested by looking at a file sent to'
                                             'la-data directory and setting off a batch delete using the API.')

parser.add_argument('-r', '--run_mode', nargs=1, type=str, choices=['interactive', 'testing'],
                    required=True, help="xAPI batch delete run mode: 'interactive' for running on a single uni, "
                                        "'testing' for running on data which not to be sent to the LDH.")
parser.add_argument('-u', '--uni_name', nargs=1, type=str, required=False, help="Institution short name.")
parser.add_argument('-st', '--start_time', nargs=1, type=str, required=False, help="Start date (format: yyyy-mm-dd)"
                                                                                   " to run the query for between two "
                                                                                   "dates, for back-filling data.")
parser.add_argument('-et', '--end_time', nargs=1, type=str, required=False, help="End date (format: yyyy-mm-dd) to"
                                                                                 " run the query for between two dates,"
                                                                                 " for back-filling data.")

# parse the arguments, convert to dict
args = vars(parser.parse_args())

# set the arguments, default = None
run_mode = args['run_mode']
interactive_site = args['uni_name']

# currently all arguments are in a list form, convert to the first element
if isinstance(run_mode, list):
    run_mode = run_mode[0]
if isinstance(interactive_site, list):
    interactive_site = interactive_site[0]

try:
    run_mode
except IndexError:
    logger.error('No run parameters found')
    raise Exception('No run parameters found')

if run_mode not in ['scheduled', 'interactive', 'testing']:
    logger.error('Run parameter invalid')
    raise Exception('Run parameter invalid')

app = 'xapi-batch-delete-' + run_mode
postEvent(app, 'started-{}'.format(run_mode), '', '', '')

# LA config inst connection
config = getLAconfig('function-uxapi-enabled')

for inst in config:
    try:
        c = (config[inst])

        xapi_username = c['uxapi-username']
        xapi_password = c['uxapi-password']
        inst_shortname = c['gen-shortname']
        ladata_dir = c['ladata-root']

    except KeyError:
        logger.error('Error returning required fields')
        continue

    # Run only for interactive site or all sites as part of scheduled process
    if run_mode == 'interactive' and inst_shortname == interactive_site or run_mode == 'testing' and inst_shortname == interactive_site:

        # select inst/input credentials
        path = ladata_dir + '/activity/delete-request/'
        file = 'deletions.tsv'

        # create new log file for each site to track errors
        if not not os.path.exists(path + 'logs/'):
            os.makedirs(path + 'logs/')

        logger.info('Starting ' + app + ': ' + inst_shortname)
        postEvent(app, 'starting-inst', inst_shortname, "", '')

        if os.path.isfile(path + file):
            try:
                deletions = pd.read_csv(path + file, sep='\t')
            except IOError:
                logger.error('File not found, ensure that the file is correctly named and in the correct directory')

            base64string = base64.encodebytes(('%s:%s' % (xapi_username, xapi_password)).encode()).decode().replace(
                '\n', '')
            xapi_headers = {'content-type': 'application/json', 'Authorization': 'Basic ' + base64string}

            xapi_batch_delete(deletions, xapi_headers)

            # log completion status
            errors = logger.logger.error.counter
            if errors > 0:
                logger.info('Finished inst with error count: {}'.format(errors))
                postEvent(run_mode, 'finished inst', inst_shortname, 'completed with errors', 'errors: {}'.format(errors))
            else:
                logger.info('Finished inst successfully')
                postEvent(run_mode, 'finished inst', inst_shortname, 'success', '')
