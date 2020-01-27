import requests
import json
import base64

from secretsmanager import getSecret
from logconfig import logger


# get site list by function enabled
def getLAconfig(function):
    laconfig = getSecret('la_config')

    url = laconfig['api_base'] + 'sites/' + function

    # get sites from uddsync-enabled
    base64string = base64.encodebytes(('%s:%s' % (laconfig['username'],
                                                  laconfig['password'])).encode()).decode().replace('\n', '')

    headers = {'content-type': 'application/json', 'Authorization': 'Basic ' + base64string}

    response = requests.get(url, headers=headers)

    return json.loads(response.text)


def gettests():
    laconfig = getSecret('la_config')
    url = laconfig['api_base'] + gettests

    base64string = base64.encodestring(('%s:%s' % (laconfig['username'],
                                                   laconfig['password'])).encode()).decode().replace('\n', '')

    headers = {'content-type': 'application/json', 'Authorization': 'Basic ' + base64string}
    response = requests.get(url, headers=headers)

    return json.loads(response.text)


# post log item to la-config
def postEvent(app, event, detail, result, data):
    laconfig = getSecret('la_config')

    base64string = base64.encodebytes(('%s:%s' % (laconfig['username'],
                                                  laconfig['password'])).encode()).decode().replace('\n', '')

    headers = {'content-type': 'application/json', 'Authorization': 'Basic ' + base64string}

    url = laconfig['postevent'] + "app=" + app +"&eventname=" + event + "&detail=" + detail + "&result=" + result + "&data=" + data
    response = requests.post(url, headers=headers)

    logger.info('Log to LA-Config: {}, {}, {}, {}'.format(event, detail, result, data))
    logger.info('Log response - Status: {}, Response: {}'.format(response.status_code, response.text))


def postMonitor(record):
    url = "https://api.la-config.data.alpha.jisc.ac.uk:443/api/monitor"

    laconfig = getSecret('la_config')

    base64string = base64.encodebytes(('%s:%s' % (laconfig['username'],
                                                  laconfig['password'])).encode()).decode().replace('\n', '')

    headers = {'content-type': 'application/json', 'Authorization': 'Basic ' + base64string}

    response = requests.post(url + record, headers=headers)
    print('Log response - Status: {}, Response: {}'.format(response.status_code, response.text))


def logquery(querystring):
    url = 'https://api.la-config.data.alpha.jisc.ac.uk/EventLog?where='
    laconfig = getSecret('la_config')
    base64string = base64.encodebytes(('%s:%s' % (laconfig['reports_basic_auth_username'],
                                                  laconfig['reports_basic_auth_password'])).encode()).decode().replace('\n', '')

    headers = {'content-type': 'application/json', 'Authorization': 'Basic ' + base64string}

    response = requests.get(url + querystring, headers=headers)
    print('Log response - Status: {}, Response: {}'.format(response.status_code, response.text))

    return json.loads(response.text)


def getUserReports():
    dataxreports = getSecret('production/datax-api/secrets')

    username = dataxreports['REPORTS_BASIC_AUTH_USERNAME']
    password = dataxreports['REPORTS_BASIC_AUTH_PASSWORD']

