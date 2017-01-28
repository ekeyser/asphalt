var express = require("express");
var router = new express.Router();
var AWS = require("aws-sdk");
var common = require("../common");
var config = common.config();
AWS.config.loadFromPath(config.aws_config_path);
var dynamodb = new AWS.DynamoDB({endpoint: new AWS.Endpoint(config.dynamodb_endpoint)});
var sqs = new AWS.SQS();
var util = require("util");
var qs = require("querystring");
var arrMessages = [];
var maxMessagesForRequesting = 10;
var maxMessagesForProcessing = 100;
var sKey;
var jsonContent;

var postConfiguration = function () {
    "use strict";

    var objParams = {
        TableName: config.dynamo_table_configuration,
        Item: {
            "configuration": {
                S: sKey
            },
            "content": {
                S: JSON.stringify(jsonContent)
            }
        }
    };

    dynamodb.putItem(objParams, function (err, data) {
        if (err) {
            console.warn(err, err.stack);
        } else {
            console.log(data);
        }
    });
};

var processMessages = function () {
    "use strict";

    var assocMessages = {};
    /*
     a bunch of messages (which may be dupes)
     to a dictionary of single instance messages
     */
    arrMessages.forEach(function (message) {
        assocMessages[message.MessageId] = message;
    });
    console.log(util.inspect(assocMessages, {showHidden: true, depth: null}));
    /*
     need to reset this array to empty so that subsequent requests
     don"t keep using the contents of this array
     */
    arrMessages = [];

    var tools = require("../tools.js");

    var assocTopicCounts = {};
    Object.keys(assocMessages).forEach(function (messageId) {
        // for (var messageId in assocMessages) {
        var s2 = JSON.parse(JSON.parse(assocMessages[messageId].Body).Message);
        console.log("s2 is...");
        console.log(util.inspect(s2, {showHidden: true, depth: null}));

        /*
         obtain the current local batch max hit count
         */
        if (assocTopicCounts[s2.data.topic] === undefined) {
            assocTopicCounts[s2.data.topic] = s2.data.hits;
        } else {
            if (s2.data.hits > assocTopicCounts[s2.data.topic]) {
                assocTopicCounts[s2.data.topic] = s2.data.hits;
            }
        }
        // }
    });
    console.log("assocTopicCounts is...");
    console.log(util.inspect(assocTopicCounts, {showHidden: true, depth: null}));

    /*
     previous batches may have had a higher hit count than our
     current batch so let"s check dynamodb/cache for source of truth
     so that we don"t accidentally overwrite cloudsearch
     with a lesser hit count. If the local value we have now
     is greater than or equal to what is in persistence then go
     ahead and use this local value to update cloudsearch since at
     least we know what we have came from dynamodb (current
     source of truth) anyway.
     */
    var arrTopics = [];
    Object.keys(assocTopicCounts).forEach(function (topic) {
        // for (var topic in assocTopicCounts) {
        arrTopics.push({
            "topic": {S: topic}
        });
        // }
    });
    var dynamodb = new AWS.DynamoDB({endpoint: new AWS.Endpoint(config.dynamodb_endpoint)});

    if (arrTopics.length > 0) {

        var params = {
            RequestItems: {},
            ReturnConsumedCapacity: "TOTAL"
        };

        params.RequestItems[dynamoTableNameTopic] = {
            AttributesToGet: ["topic", "hits"],
            Keys: arrTopics
        };

        console.log("mk17");
        console.log(util.inspect(params, {showHidden: true, depth: null}));
        console.log("mk18");
        dynamodb.batchGetItem(params, function (err, data) {
            console.log("mk19");
            if (err) {
                console.warn(err, err.stack);
            } else {
                console.log("mk20");
                console.log(util.inspect(data, {showHidden: true, depth: null}));
                var arrBatch = [];
                console.log("assocTopicCounts is...");
                console.log(util.inspect(assocTopicCounts, {showHidden: true, depth: null}));
                data.Responses[dynamoTableNameTopic].forEach(function (dynamoRecord) {
                    console.log("dynamoRecord is...");
                    console.log(util.inspect(dynamoRecord, {showHidden: true, depth: null}));
                    console.log("assocTopicCounts[dynamoRecord.topic.S] is...");
                    console.log(util.inspect(assocTopicCounts[dynamoRecord.topic.S], {showHidden: true, depth: null}));
                    if (assocTopicCounts[dynamoRecord.topic.S] >= parseInt(dynamoRecord.hits.N)) {
                        var batchItem = {
                            type: "add",
                            id: qs.escape(dynamoRecord.topic.S),
                            fields: {
                                topic: dynamoRecord.topic.S,
                                hits: assocTopicCounts[dynamoRecord.topic.S]
                            }
                        };
                        console.log("mk21");
                        arrBatch.push(batchItem);
                    }

                    console.log("mk24");

                    // if (arrBatch.length > 0) {
                    //     console.log("mk22");
                    //     tools.cloudsearch(arrBatch, AWS, assocMessages);
                    // } else {
                    //     console.log("mk23");
                    //     Object.keys(assocMessages).forEach(function (messageId) {
                    //         var message = assocMessages[messageId];
                    //         var receipthandle = message.ReceiptHandle;
                    //         sqs.deleteMessage({
                    //             QueueUrl: config.notify_cloudsearch,
                    //             "ReceiptHandle": receipthandle
                    //         }, function (err, data) {
                    //             if (err) {
                    //                 console.warn(err, err.stack);
                    //             }
                    //         });
                    //     });
                    // }
                });

                if (arrBatch.length > 0) {
                    console.log("mk22");
                    tools.cloudsearch(arrBatch, AWS, assocMessages);
                } else {
                    console.log("mk23");
                    Object.keys(assocMessages).forEach(function (messageId) {
                        var message = assocMessages[messageId];
                        var receipthandle = message.ReceiptHandle;
                        sqs.deleteMessage({
                            QueueUrl: config.notify_cloudsearch,
                            "ReceiptHandle": receipthandle
                        }, function (err, data) {
                            if (err) {
                                console.warn(err, err.stack);
                            }
                        });
                    });
                }
            }
        });
    }
};

var retrieve = function () {
    "use strict";

    sqs.receiveMessage({
        QueueUrl: config.notify_cloudsearch,
        "MaxNumberOfMessages": maxMessagesForRequesting
    }, function (err, data) {
        if (err) {
            console.warn(err, err.stack);
        }
        else {
            if (data.Messages !== undefined) {
                data.Messages.forEach(function (message) {
                    console.log(util.inspect(message, {showHidden: true, depth: null}));
                    arrMessages.push(message);
                });
                /*
                 can we get more?
                 */
                if (arrMessages.length < maxMessagesForProcessing) {
                    sqs.getQueueAttributes({
                        QueueUrl: config.notify_cloudsearch,
                        AttributeNames: ["ApproximateNumberOfMessages"]
                    }, function (err, data) {
                        if (err) {
                            console.warn(err, err.stack);
                        } else {
                            if (data.Attributes.ApproximateNumberOfMessages > 0) {
                                retrieve();
                            } else {
                                processMessages();
                            }
                        }
                    });
                } else {
                    processMessages();
                }
            } else {
                processMessages();
            }
        }
    });
};

router.post("/", function (req, res, next) {
    "use strict";

    console.log(util.inspect(req.body, {showHidden: true, depth: null}));
    sKey = req.body.key;
    console.log("Mk1");
    // jsonContent = JSON.parse(req.body.configuration);
    jsonContent = req.body.configuration;
    console.log("Mk2");
    console.log(jsonContent);
    console.log("Mk3");

    postConfiguration();
    // if (req.headers["x-amz-sns-message-type"] === "SubscriptionConfirmation") {
    //     var tools = require("../tools.js");
    //     tools.confirmSNSSubscription(req, AWS);
    // } else {
    //     retrieve();
    // }
    res.send(JSON.stringify({response: "OK"}));
});

module.exports = router;
