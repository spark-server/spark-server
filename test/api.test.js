'use strict';
const httpStatus = require('http-status');
const chai = require('chai');
const expect = chai.expect;
const app = require('../spark-server');
const supertest = require('supertest-as-promised');
const request = supertest(app);

chai.config.includeStack = false;


describe('# Check Spark-Server functionality', function() {

    var contextName = 'mycontext',
        sessionId = null;

    describe('## Create Context with standard settings', function () {

        this.timeout(30000);

        var contextBody = {
            "config": {
                "createSQLContext": true,
                "properties": {
                    "spark.driver.cores": "1",
                    "spark.driver.memory": "1g",
                    "spark.executor.memory": "2g",
                    "spark.shuffle.sort.bypassMergeThreshold": "8"
                },
                "packages": [
                    "com.databricks:spark-csv_2.10:1.3.0"
                ]
            }
        };

        it('should return a 201 when the Context is created', function () {

            return request
                .post('/v1/contexts/'+contextName)
                .send(contextBody)
                .expect(httpStatus.CREATED)

        });

    });

    describe('## Create Session in Context', function () {

        it('should return a 201 when the Session is created', function () {

            return request
                .post('/v1/contexts/'+contextName+'/sessions')
                .expect(httpStatus.CREATED)
                .then(function(session) {
                    sessionId = JSON.parse(session.text).session.id;
                })

        });

    });

    describe('## Run statement in Session', function () {

        this.timeout(30000);

        var statement = {
            "code": [
                "var people = sqlContext.read().format('com.databricks.spark.csv').option('header', 'true').option('inferSchema', 'true').option('delimiter', ',').load(getFileById('people.csv'))"
            ],
            "return": "people.head(10)"
        };

        it('should return the correct result (length of 10) when the Statement is executed', function () {

            return request
                .post('/v1/contexts/'+contextName+'/sessions/'+sessionId+'/statements')
                .send(statement)
                .expect(httpStatus.OK)
                .then(function(result) {

                    var people = JSON.parse(result.text).result;

                    expect(people).to.be.a('array');
                    expect(people).to.have.length(10);
                })

        });

    });

    describe('## Destroy Session', function () {

        it('should return 200 if the Session is destroyed', function () {

            return request
                .delete('/v1/contexts/'+contextName+'/sessions/'+sessionId)
                .expect(httpStatus.OK)

        });

    });

    describe('## Destroy Context', function () {

        it('should return 200 if the Context is destroyed', function () {

            return request
                .delete('/v1/contexts/'+contextName)
                .expect(httpStatus.OK)

        });

    });

});
