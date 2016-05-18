import debug from 'debug'
import url from 'url'
import path from 'path'
import fs from 'fs'
import request from 'request'
import rp from 'request-promise'
import xml2js from 'xml2js'
import Promise from 'bluebird'
import shelljs from 'shelljs'
import dotenv from 'dotenv'
dotenv.config()

const sh = shelljs
const dbg = debug('BulkClient')

const wrappedRp = (opts) => {
    return rp(opts)
        .then(res => {
            dbg(res)
            return res
        })
}

export class BulkClient {
    constructor(opts) {
        this.opts = opts
    }

    request(opts) {
        return wrappedRp(opts)
    }

    get defaultHeaders() {
        return {
            'X-SFDC-Session': this.loginResponse.sessionId,
            'Content-Type': 'application/xml; charset=UTF-8'
        }
    }

    login() {
        const xml = `<?xml version="1.0" encoding="utf-8" ?>
<env:Envelope xmlns:xsd="http://www.w3.org/2001/XMLSchema"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
  <env:Body>
    <n1:login xmlns:n1="urn:partner.soap.sforce.com">
      <n1:username>${this.opts.username}</n1:username>
      <n1:password>${this.opts.password}</n1:password>
    </n1:login>
  </env:Body>
</env:Envelope>`

        const options = {
            url: `${this.opts.serverUrl}/services/Soap/u/${this.opts.apiVersion}`,
            method: 'POST',
            headers: {
                'Content-Type': 'text/xml; charset=UTF-8',
                'SOAPAction': 'login'
            },
            body: xml
        }

        return this.request(options)
            .then(res => this.parseXmlToJs(res))
            .then(res => {
                this.loginResponse = res["soapenv:Envelope"]["soapenv:Body"][0]["loginResponse"][0]["result"][0]
                const u = url.parse(this.loginResponse.serverUrl[0])
                this.instanceUrl = `${u.protocol}//${u.host}`
                dbg('instanceUrl', this.instanceUrl)
                return this.loginResponse
            })
    }

    createJob() {
        const xml = `<?xml version="1.0" encoding="UTF-8"?>
<jobInfo
    xmlns="http://www.force.com/2009/06/asyncapi/dataload">
  <operation>query</operation>
  <object>Address_vod__c</object>
  <concurrencyMode>Parallel</concurrencyMode>
  <contentType>CSV</contentType>
</jobInfo>`

        const opts = {
            uri: `${this.instanceUrl}/services/async/${this.opts.apiVersion}/job`,
            method: 'POST',
            headers: this.defaultHeaders,
            body: xml
        }


        return this.request(opts)
            .then(res => this.parseAndUpdateJob(res))
    }

    parseAndUpdateJob(res) {

        return this.parseXmlToJs(res)
            .then(res => {
                this.job = res.jobInfo
                this.job.id = this.job.id[0]
                return this.job
            })
    }

    parseAndUpdateBatch(res) {

        return this.parseXmlToJs(res)
            .then(res => {
                const batch = res.batchInfo
                batch.id = batch.id[0]
                this.batches = this.batches || []

                const matchedBatches = this.batches.filter(b => b.id === batch.id)
                if (matchedBatches.length > 0) {
                    matchedBatches[0] = batch
                } else {
                    this.batches.push(batch)
                }

                return batch
            })
    }

    parseAndUpdateBatchResult(batchId, res) {
        return this.parseXmlToJs(res)
            .then(res => {
                const matchedBatches = this.batches.filter(b => b.id === batchId)
                matchedBatches[0].resultIds = res['result-list']['result']
                return res
            })
    }

    addBatch() {
        dbg('addBatch')
        const opts = {
            uri: `${this.instanceUrl}/services/async/${this.opts.apiVersion}/job/${this.job.id}/batch`,
            method: 'POST',
            headers: Object.assign({}, this.defaultHeaders, {'Content-Type': 'text/csv; charset=UTF-8'}),
            body: 'SELECT Id, Name FROM Address_vod__c'
        }

        return this.request(opts)
            .then(res => this.parseAndUpdateBatch(res))
    }

    jobStatus(id) {
        const opts = {
            uri: `${this.instanceUrl}/services/async/${this.opts.apiVersion}/job/${this.job.id}`,
            method: 'GET',
            headers: this.defaultHeaders
        }

        return this.request(opts)
            .then(res => this.parseAndUpdateJob(res))
    }

    batchStatus(id) {
        const opts = {
            uri: `${this.instanceUrl}/services/async/${this.opts.apiVersion}/job/${this.job.id}/batch/${id}`,
            method: 'GET',
            headers: this.defaultHeaders
        }

        return this.request(opts)
            .then(res => this.parseAndUpdateBatch(res))
    }

    batchResultId(batchId) {
        const opts = {
            uri: `${this.instanceUrl}/services/async/${this.opts.apiVersion}/job/${this.job.id}/batch/${batchId}/result`,
            method: 'GET',
            headers: this.defaultHeaders
        }

        return this.request(opts)
            .then(res => this.parseAndUpdateBatchResult(batchId, res))
    }

    batchResult(batchId, batchResultId) {
        const opts = {
            uri: `${this.instanceUrl}/services/async/${this.opts.apiVersion}/job/${this.job.id}/batch/${batchId}/result/${batchResultId}`,
            method: 'GET',
            headers: this.defaultHeaders
        }

        return this.request(opts)
            .then(res => {
                const outputPath = this.outputFilePath(batchId, batchResultId)
                sh.mkdir('-p', path.dirname(outputPath))
                fs.writeFileSync(outputPath, res)

                return res
            })
    }

    outputFilePath(batchId, batchResultId) {
        return path.resolve(this.opts.outputDirectory, this.job.id, `${batchId}-${batchResultId}.csv`)
    }

    closeJob() {
        const xml = `<?xml version="1.0" encoding="UTF-8"?>
<jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
  <state>Closed</state>
</jobInfo>`

        const opts = {
            uri: `${this.instanceUrl}/services/async/${this.opts.apiVersion}/job/${this.job.id}`,
            method: 'POST',
            headers: this.defaultHeaders,
            body: xml
        }


        return this.request(opts)
            .then(res => this.parseAndUpdateJob(res))


    }

    waitForJobToComplete() {
        dbg('waitForJobToComplete job.id', this.job.id)
        return this.jobStatus(this.job.id)
            .then(res => {
                if ((parseInt(res.numberBatchesCompleted[0]) + parseInt(res.numberBatchesFailed[0])) === parseInt(res.numberBatchesTotal[0])) {
                    dbg('job complete')
                    return res
                } else {

                    return Promise.delay(10000)
                        .then(() => this.waitForJobToComplete())
                }
            })
    }

    fetchBatchResultIds() {
        dbg('fetchBatchResultIds')
        const promises = this.batches.map(b => this.batchResultId(b.id))
        return Promise.all(promises)
    }

    fetchBatchResults() {
        dbg('fetchBatchResults')
        const promises = this.batches.map(b => Promise.all(b.resultIds.map(resultId => this.batchResult(b.id, resultId))))
        return Promise.all(promises)
    }

    query() {
        dbg('query')
        return this.createJob()
            .then(res => this.addBatch())
            .then(res => this.waitForJobToComplete())
            .then(res => this.fetchBatchResultIds())
            .then(res => this.fetchBatchResults())
            .then(res => this.closeJob())
            .then(res => dbg('fetchBatchResultIds', res))
    }

    parseXmlToJs(xml) {
        return new Promise((resolve, reject) => {
            xml2js.Parser().parseString(xml, (err, res) => {
                if (err) {
                    reject(err)
                }
                resolve(res)

            })
        })
    }
}
