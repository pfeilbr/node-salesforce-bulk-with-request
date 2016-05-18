import { BulkClient } from '../'
import { expect } from 'chai'
import dotenv from 'dotenv'
dotenv.load()


describe('BulkClient', () => {
    let client = null
    beforeEach(done => {
        const serverUrl = process.env.URL
        const apiVersion = process.env.API_VERSION
        const username = process.env.USERNAME
        const password = process.env.PASSWORD
        const outputDirectory = process.env.OUTPUT_DIRECTORY

        client = new BulkClient({serverUrl, apiVersion, username, password, outputDirectory})

        done()
    })

    it('should login', done => {

        client.login()
            .then(res => {
                expect(res.sessionId[0]).to.be.a('string')
                expect(res.sessionId[0].length).to.be.above(0)
                //console.log(JSON.stringify(res, null, 2))
                done()
            })
            .catch(done)

    })

    it('should query', done => {

        client.login()
            .then(() => client.query())
            .then(res => {
                
                done()
            })
            .catch(done)

    })

})