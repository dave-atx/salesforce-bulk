from __future__ import absolute_import

# Interface to the Salesforce BULK API

import requests
import StringIO
import time
import urlparse
import xml.etree.ElementTree as ET

from . import bulk_states

DEFAULT_BATCH_SIZE = 2500


class BulkApiError(Exception):
    def __init__(self, message, status_code=None):
        super(BulkApiError, self).__init__(message)
        self.status_code = status_code


class BulkJobAborted(BulkApiError):
    def __init__(self, job_id):
        self.job_id = job_id

        message = 'Job {0} aborted'.format(job_id)
        super(BulkJobAborted, self).__init__(message)


class BulkBatchFailed(BulkApiError):
    def __init__(self, job_id, batch_id, state_message):
        self.job_id = job_id
        self.batch_id = batch_id
        self.state_message = state_message

        message = 'Batch {0} of job {1} failed: {2}'.format(batch_id, job_id,
                                                            state_message)
        super(BulkBatchFailed, self).__init__(message)


class Batch(object):
    def __init__(self, session, job_endpoint, batch_id):
        self.session = session
        self.endpoint = "{}/batch/{}".format(job_endpoint, batch_id)
        self.batch_id = batch_id
        self._status = None

    def status(self, reload=False):
        if reload or self._status is None:
            resp = self.session.get(self.endpoint)
            SalesforceBulk.check_status(resp)
            self._status = resp.json()
        return self._status

    def is_done(self, reload=False):
        batch_status = self.status(reload)
        batch_state = batch_status['state'] if 'state' in batch_status else None
        if batch_state in bulk_states.ERROR_STATES:
            raise BulkBatchFailed(batch_status['jobId'], batch_status['id'],
                                  batch_status['stateMessage'])
        return batch_state == bulk_states.COMPLETED

    # Wait for all batches in this job to complete, waiting at most timeout seconds
    # (defaults to 10 minutes).
    def wait(self, timeout=60 * 10, sleep_interval=10):
        waited = 0
        while not self.is_done(True) and waited < timeout:
            time.sleep(sleep_interval)
            waited += sleep_interval

    def results(self):
        if not self.is_done():
            self.wait()
        uri = self.endpoint + '/result'
        resp = self.session.get(uri)
        SalesforceBulk.check_status(resp)
        for row in resp.json():
            yield row


class BatchQuery(Batch):
    def __init__(self, session, job_endpoint, batch_id):
        Batch.__init__(self, session, job_endpoint, batch_id)

    def results(self):
        if not self.is_done():
            self.wait()
        uri = self.endpoint + '/result'
        resp = self.session.get(uri)
        SalesforceBulk.check_status(resp)
        result_ids = resp.json()
        for result_id in result_ids:
            uri = "{}/result/{}".format(self.endpoint, result_id)
            resp = self.session.get(uri)
            SalesforceBulk.check_status(resp)
            for row in resp.json():
                yield row


class Job(object):
    def __init__(self, session, endpoint, job_id):
        self.session = session
        self.endpoint = "{}/job/{}".format(endpoint, job_id)
        self.job_id = job_id
        self.batches = []

    def close(self):
        """Close this job."""
        resp = self.session.post(self.endpoint, json={'state': 'Closed'})
        SalesforceBulk.check_status(resp)

    def abort(self):
        """Abort this job."""
        resp = self.session.post(self.endpoint, json={'state': 'Aborted'})
        SalesforceBulk.check_status(resp)

    def query(self, soql):
        """Create a batch query."""
        uri = self.endpoint + "/batch"
        resp = self.session.post(uri, data=soql)
        SalesforceBulk.check_status(resp)
        batch = BatchQuery(self.session, self.endpoint, resp.json()['id'])
        self.batches.append(batch)
        return batch

    def post(self, data, batch_size=DEFAULT_BATCH_SIZE):
        """Create a batch upload."""
        uri = self.endpoint + "/batch"
        batches = []
        # create one batch for each set of batch_size rows in data
        for i in xrange(0, len(data), batch_size):
            resp = self.session.post(uri, json=data[i:i+batch_size])
            SalesforceBulk.check_status(resp)
            batch = Batch(self.session, self.endpoint, resp.json()['id'])
            self.batches.append(batch)
            batches.append(batch)
        return batches

    def results(self):
        if not self.is_done():
            self.wait()
        for batch in self.batches:
            for row in batch.results():
                yield row

    def is_done(self):
        for batch in self.batches:
            if not batch.is_done():
                return False
        return True

    # Wait for all batches in this job to complete, waiting at most timeout seconds
    # (defaults to 10 minutes).
    def wait(self, timeout=60 * 10, sleep_interval=10):
        for batch in self.batches:
            batch.wait(timeout, sleep_interval)

    def __enter__(self):
        return self

    def __exit__(self, exc, value, traceback):
        self.close()


class SalesforceBulk(object):
    def __init__(self, sessionId=None, host='login.salesforce.com',
                 username=None, password=None,
                 exception_class=BulkApiError, API_version="36.0"):
        if not sessionId and not username:
            raise RuntimeError(
                "Must supply either sessionId/instance_url or username/password")

        host, sessionId = SalesforceBulk.login(username, password, host, API_version)

        self.endpoint = "https://{}/services/async/{}".format(host, API_version)
        self.session = requests.Session()
        self.session.headers["X-SFDC-Session"] = sessionId
        self.session.headers["Content-Type"] = "application/json"

    def create_query_job(self, object_name, **kwargs):
        return self.create_job(object_name, "query", **kwargs)

    def create_insert_job(self, object_name, **kwargs):
        return self.create_job(object_name, "insert", **kwargs)

    def create_upsert_job(self, object_name, external_id_name, **kwargs):
        return self.create_job(object_name, "upsert", external_id_name=external_id_name, **kwargs)

    def create_update_job(self, object_name, **kwargs):
        return self.create_job(object_name, "update", **kwargs)

    def create_delete_job(self, object_name, **kwargs):
        return self.create_job(object_name, "delete", **kwargs)

    def create_job(self, object_name=None, operation=None,
                   concurrency=None, external_id_name=None):
        assert(object_name is not None)
        assert(operation is not None)

        description = {'operation': operation, 'object': object_name, 'contentType': 'JSON'}
        if external_id_name:
            description['externalIdFieldName'] = external_id_name
        if concurrency:
            description['concurrencyMode'] = concurrency

        resp = self.session.post(self.endpoint + "/job", json=description)
        SalesforceBulk.check_status(resp)
        return Job(self.session, self.endpoint, resp.json()['id'])

    @staticmethod
    def login(username, password, host, API_version):
        """Login to salesforce.com. Returns a tuple of the server hostname and session ID.
        Args:
            username - SFDC username
            password - SFDC password + server token concatenated
            host - SFDC login host - either login.salesforce.com or test.salsesforce.com
        """
        LOGIN = ('<?xml version="1.0" encoding="utf-8" ?>'
                 '<env:Envelope xmlns:xsd="http://www.w3.org/2001/XMLSchema"'
                 '    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
                 '    xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">'
                 '  <env:Body>'
                 '    <n1:login xmlns:n1="urn:partner.soap.sforce.com">'
                 '      <n1:username>{username}</n1:username>'
                 '      <n1:password>{password}</n1:password>'
                 '    </n1:login>'
                 '  </env:Body>'
                 '</env:Envelope>')
        SERVER_URL = './/{urn:partner.soap.sforce.com}serverUrl'
        SESSION_ID = './/{urn:partner.soap.sforce.com}sessionId'
        headers = {'Content-Type': 'text/xml; charset=UTF-8', 'SOAPAction': 'login'}
        payload = LOGIN.format(username=username, password=password)
        login_endpoint = 'https://{}/services/Soap/u/{}'.format(host, API_version)
        r = requests.post(login_endpoint, data=payload, headers=headers)
        tree = ET.parse(StringIO.StringIO(r.content))
        return (urlparse.urlparse(tree.find(SERVER_URL).text).hostname, tree.find(SESSION_ID).text)

    @staticmethod
    def check_status(response):
        if response.status_code >= 400:
            msg = "Bulk API HTTP Error result: {0}".format(response.text)
            raise BulkApiError(msg, response.status)
